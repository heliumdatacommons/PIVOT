from tornado.escape import url_escape

from scheduler.appliance import DefaultApplianceScheduler
from scheduler.appliance.base import SchedulePlan
from cluster.manager import ClusterManager
from container.manager import ContainerDBManager
from commons import APIManager
from config import config


class LocationAwareApplianceScheduler(DefaultApplianceScheduler):

  def __init__(self, app):
    super(LocationAwareApplianceScheduler, self).__init__(app)
    self.__api = iRODSAPIManager()
    self.__cluster_mgr = ClusterManager()
    self.__contr_db = ContainerDBManager()

  async def schedule(self, plans):
    if not config.irods.host or not config.irods.port:
      self.logger.info('iRODS API is not properly set. Fallback to default scheduler')
      super(LocationAwareApplianceScheduler, self).schedule(plans)
      return
    assert self.dag is not None
    await self.ensure_appliance_exist()
    for p in plans.values():
      if p.failed:
        self.logger.info("Plan '%s' Failed"%p.id)
        p.stop()
        continue
      for c in p.provisioned:
        self.dag.update_container(c)
      for c in p.done:
        self.dag.remove_container(c.id)
    self.logger.info("Update the DAG in the database")
    status, msg, err = await self.sched_mgr.update_appliance_dag(self.dag)
    if err:
      self.logger.error(err)
    contrs = self.dag.get_free_containers()
    self.logger.info('Free containers: %s'%[c.id for c in contrs])
    new_plans = []
    for c in contrs:
      if c.id in plans:
        continue
      if c.data:
        regions = {}
        for lfn in c.data.input:
          status, data_obj, err = await self.__api.get_data_object(lfn)
          if status != 200:
            self.logger.error(err)
            continue
          for r in await self.__api.get_replica_regions(data_obj.get('replicas', [])):
            regions[r] = regions.setdefault(r, 0) + data_obj.get('size', 0)
        agents = [agent for agent in
                  await self.__cluster_mgr.find_agents(
                    region={'$in': list(regions.keys())}, ttl=0)
                  if agent.resources.cpus >= c.resources.cpus
                  and agent.resources.mem >= c.resources.mem
                  and agent.resources.disk >= c.resources.disk]
        if agents:
          self.logger.info('Candidate regions:')
          for a in agents:
            region = a.attributes.get('region')
            cloud = a.attributes.get('cloud')
            data_size = regions.get(region, 0)
            self.logger.info('\t%s, %s, data size: %d'%(region, cloud, data_size))
          agent = max(agents, key=lambda a: regions.get(a.attributes.get('region'), 0))
          cloud, region = agent.attributes.get('cloud'), agent.attributes.get('region')
          self.logger.info("Container '%s' will land on %s (%s, %s)"%(c.id, agent.hostname,
                                                                      region, cloud))
          c.host = agent.hostname
        else:
          self.logger.info("No matched agents have sufficient resources for '%s'"%c)
      await self.__contr_db.save_container(c, False)
      new_plans += SchedulePlan(c.id, [c]),
    if new_plans:
      self.logger.info('New plans: %s'%[p.id for p in new_plans])
    return new_plans


class iRODSAPIManager(APIManager):

  def __init__(self):
    super(iRODSAPIManager, self).__init__()

  async def get_data_object(self, lfn):
    api = config.irods
    endpoint = '%s/getDataObject?filename=%s'%(api.endpoint, url_escape(lfn))
    status, data_obj, err = await self.http_cli.get(api.host, api.port, endpoint)
    if status != 200:
      return status, None, err
    return status, data_obj, None

  async def get_replica_regions(self, replicas):
    api = config.irods
    locations = []
    for r in replicas:
      endpoint = '%s/getResourceMetadata?resource_name=%s'%(api.endpoint,
                                                            url_escape(r['resource_name']))
      status, resc, err = await self.http_cli.get(api.host, api.port, endpoint)
      if status != 200:
        self.logger.error(err)
        continue
      locations.append(resc['region'])
    return locations

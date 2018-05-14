from tornado.escape import url_escape

from scheduler import DefaultApplianceScheduler
from scheduler.base import SchedulePlan
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
      if c.input_data:
        status, locations, err = await self.__api.get_replica_locations(c.input_data[0])
        if status != 200:
          self.logger.error(err)
          continue
        agents = [agent for agent in
                  await self.__cluster_mgr.find_agents(region=locations[0],ttl=0)
                  if agent.resources.cpus >= c.resources.cpus
                  and agent.resources.mem >= c.resources.mem
                  and agent.resources.disk >= c.resources.disk]
        if not agents:
          self.logger.info("No matched agents have sufficient resources for '%s'"%c)
          continue
        self.logger.info("Container '%s' will land on %s"%(c.id, agents[0].hostname))
        c.host = agents[0].hostname
      self.__contr_db.save_container(c, False)
      new_plans += SchedulePlan(c.id, [c]),
    if new_plans:
      self.logger.info('New plans: %s'%[p.id for p in new_plans])
    return new_plans


class iRODSAPIManager(APIManager):

  def __init__(self):
    super(iRODSAPIManager, self).__init__()

  async def get_replica_locations(self, lfn):
    api = config.irods
    endpoint = '%s/getReplicas?filename=%s'%(api.endpoint, url_escape(lfn))
    status, replicas, err = await self.http_cli.get(api.host, api.port, endpoint)
    if status != 200:
      return status, None, err
    locations = []
    for r in replicas['replicas']:
      endpoint = '%s/getResourceMetadata?resource_name=%s'%(api.endpoint,
                                                            url_escape(r['resource_name']))
      status, resc, err = await self.http_cli.get(api.host, api.port, endpoint)
      if status != 200:
        self.logger.error(err)
        continue
      locations.append(resc['region'])
    return 200, locations, None


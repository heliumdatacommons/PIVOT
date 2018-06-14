from tornado.escape import url_escape

from config import config
from commons import APIManager
from schedule.local import DefaultApplianceScheduler


class LocationAwareApplianceScheduler(DefaultApplianceScheduler):

  def __init__(self):
    super(LocationAwareApplianceScheduler, self).__init__()
    self.__api = iRODSAPIManager()

  async def schedule(self, app, agents):
    sched = await super(LocationAwareApplianceScheduler, self).schedule(app, agents)
    if sched.done:
      return sched
    if not config.irods.host or not config.irods.port:
      self.logger.info('iRODS API is not properly set. Fallback to default scheduler')
      return sched
    for c in sched.containers:
      if not c.data or not c.data.input: continue
      regions = {}
      for lfn in c.data.input:
        data_obj = await self._get_data_object(lfn)
        if not data_obj: continue
        for r in await self.__api.get_replica_regions(data_obj.get('replicas', [])):
          regions[r] = regions.setdefault(r, 0) + data_obj.get('size', 0)
      agents = [a for a in agents
                if a.attributes.get('region', None) in regions.keys()
                and a.resources.cpus >= c.resources.cpus
                and a.resources.mem >= c.resources.mem
                and a.resources.disk >= c.resources.disk]
      if not agents:
        self.logger.info("No matched agents have sufficient resources for '%s'"%c)
        return sched
      self.logger.info('Candidate regions:')
      for a in agents:
        region, cloud = a.attributes.get('region'), a.attributes.get('cloud')
        data_size = regions[region]
        self.logger.info('\t%s, %s, data size: %d'%(region, cloud, data_size))
      agent = max(agents, key=lambda a: regions.get(a.attributes.get('region'), 0))
      self.logger.info("Container '%s' will land on %s (%s, %s)"%(
        c.id, agent.hostname, agent.attributes['region'], agent.attributes['cloud']))
      c.schedule.add_constraint('hostname', agent.hostname)
    return sched

  async def _get_data_object(self, lfn):
    status, data_obj, err = await self.__api.get_data_object(lfn)
    if status != 200:
      self.logger.error(err)
    return data_obj


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

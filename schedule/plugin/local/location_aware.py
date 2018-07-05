from tornado.escape import url_escape

from config import config
from commons import APIManager
from schedule.local import DefaultApplianceScheduler


class LocationAwareApplianceScheduler(DefaultApplianceScheduler):

  def __init__(self, config):
    super(LocationAwareApplianceScheduler, self).__init__(config)
    self._validate_scheduler_config()
    self.__api = iRODSAPIManager(config['irods']['host'],
                                 config['irods']['port'],
                                 config['irods']['endpoint'])

  async def schedule(self, app, agents):
    sched = await super(LocationAwareApplianceScheduler, self).schedule(app, list(agents))
    if sched.done:
      return sched
    files, data_objs, resources = set(), {}, {}
    for c in sched.containers:
      if not c.data or not c.data.input: continue
      files.update([lfn for lfn in c.data.input])
    if files:
      data_objs = {d['path']: d for d in await self.__api.get_data_objects(files)}
      resource_names = set([repl['resource_name'] for d in data_objs.values()
                            for repl in d['replicas']])
      resources = {r['name']: r for r in await self.__api.get_resources(resource_names)}
    for c in sched.containers:
      if not c.data or not c.data.input: continue
      regions = {}
      for lfn in c.data.input:
        data_obj = data_objs.get(lfn)
        if not data_obj: continue
        for repl in data_obj['replicas']:
          r = resources.get(repl['resource_name'])
          if r:
            regions[r['region']] = regions.setdefault(r['region'], 0) + data_obj['size']
      matched = [a for a in agents
                 if a.attributes.get('region') in regions]
      if not matched:
        self.logger.info("No matched agents found for '%s'"%c)
        continue
      self.logger.info('Candidate regions:')
      for a in matched:
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

  def _validate_scheduler_config(self):
    irods = self.config.get('irods')
    if not isinstance(irods, dict) \
        or not isinstance(irods.get('host'), str) \
        or not isinstance(irods.get('port'), int) \
        or not isinstance(irods.get('endpoint'), str):
      raise Exception('iRODS is not properly configured, fall back to default scheduler')
    scale = self.config.get('scale')
    if not isinstance(scale, bool):
      self.config['scale'] = False


class iRODSAPIManager(APIManager):

  def __init__(self, host, port, endpoint):
    super(iRODSAPIManager, self).__init__()
    self.__host = host
    self.__port = port
    self.__endpoint = endpoint

  async def get_data_object(self, lfn):
    endpoint = '%s/getDataObject?filename=%s'%(self.__endpoint, url_escape(lfn))
    status, data_obj, err = await self.http_cli.get(self.__host, self.__port, endpoint)
    if status != 200:
      return status, None, err
    return status, data_obj, None

  async def get_data_objects(self, filenames):
    endpoint = '%s/getDataObjects?filenames=%s'%(self.__endpoint,
                                                 url_escape(','.join(filenames)))
    status, data_objs, err = await self.http_cli.get(self.__host, self.__port, endpoint)
    if err:
      self.logger.error(err)
      return []
    return data_objs

  async def get_resources(self, resource_names):
    resource_names = ','.join(resource_names)
    endpoint = '%s/getResourcesMetadata?resource_names=%s'%(self.__endpoint,
                                                            url_escape(resource_names))
    status, resources, err = await self.http_cli.get(self.__host, self.__port, endpoint)
    if err:
      self.logger.error(err)
      return []
    return resources

  async def get_replica_regions(self, replicas):
    locations = []
    for r in replicas:
      endpoint = '%s/getResourceMetadata?resource_name=%s'%(self.__endpoint,
                                                            url_escape(r['resource_name']))
      status, resc, err = await self.http_cli.get(self.__host, self.__port, endpoint)
      if status != 200:
        self.logger.error(err)
        continue
      locations.append(resc['region'])
    return locations

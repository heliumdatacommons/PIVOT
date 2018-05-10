from config import config
from cluster.base import Host, HostResources
from commons import Singleton, MotorClient, Loggable, APIManager, AutonomousMonitor


class ClusterManager(Loggable, metaclass=Singleton):

  MONITOR_INTERVAL = 30000

  def __init__(self):
    self.__cluster_db = ClusterDBManager()
    self.__cluster_api = ClusterAPIManager()
    self.__cluster_monitor = ClusterMonitor(self.__cluster_api, self.__cluster_db)

  async def get_cluster(self):
    return await self.__cluster_db.get_cluster()

  async def find_hosts(self, **kwargs):
    return await self.__cluster_db.find_hosts(**kwargs)

  def start_monitor(self):
    self.__cluster_monitor.start()


class ClusterMonitor(AutonomousMonitor):
  
  def __init__(self, api, db):
    super(ClusterMonitor, self).__init__(30000)
    self.__api = api
    self.__db = db
  
  async def callback(self):
    status, master, err = await self.__api.get_current_master()
    if status != 200:
      self.logger.error(err)
      self.stop()
      raise Exception('Failed to get leader master')
    if master != config.mesos.host:
      config.mesos.host, config.marathon.host = master, master
      self.logger.info('Current leader: %s:%d'%(config.mesos.host, config.mesos.port))
    status, hosts, err = await self.__api.get_host_updates()
    if status != 200:
      self.logger.info(err)
      return
    await self.__db.update_hosts(hosts)


class ClusterAPIManager(APIManager):

  async def get_current_master(self):
    api = config.marathon
    status, body, err = await self.http_cli.get(api.host, api.port,
                                                '%s/leader'%api.endpoint)
    if status != 200:
      return status, None, err
    return status, body.get('leader', '').split(':')[0], None

  async def get_host_updates(self):
    api = config.mesos
    status, body, err = await self.http_cli.get(api.host, api.port,
                                                '%s/master/slaves'%api.endpoint)
    if status != 200:
      return status, None, err
    hosts = [Host(hostname=h['hostname'],
                  resources=HostResources(h['resources']['cpus'],
                                          h['resources']['mem'],
                                          h['resources']['disk'],
                                          h['resources']['gpus'],
                                          h['resources']['ports'][1:-1].split(',')),
                  attributes=h['attributes'])
             for h in body['slaves']]
    return status, hosts, None


class ClusterDBManager(Loggable, metaclass=Singleton):

  def __init__(self):
    self.__host_col = MotorClient().requester.host

  async def get_cluster(self):
    return [Host(**h, resources=HostResources(**h.pop('resources', None)))
            async for h in self.__host_col.find()]

  async def find_hosts(self, **kwargs):
    cond = {'attributes.%s'%k: {'$in': v} if isinstance(v, list) else v
            for k, v in kwargs.items()}
    return [Host(**h, resources=HostResources(**h.pop('resources', None)))
            async for h in self.__host_col.find(cond)]

  async def update_hosts(self, hosts):
    for h in hosts:
      await self.__host_col.replace_one(dict(hostname=h.hostname),
                                        h.to_save(), upsert=True)


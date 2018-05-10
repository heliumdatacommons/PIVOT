import datetime

from config import config
from cluster.base import Host, HostResources
from commons import Singleton, MotorClient, Loggable, APIManager, AutonomousMonitor


class ClusterManager(Loggable, metaclass=Singleton):

  def __init__(self, monitor_interval=30000):
    cluster_api, cluster_db = ClusterAPIManager(), ClusterDBManager()
    self.__cluster_api, self.__cluster_db = cluster_api, cluster_db
    self.__cluster_monitor = ClusterMonitor(cluster_api, cluster_db, monitor_interval)

  async def get_cluster(self, ttl=30):
    if self._is_cache_expired(ttl):
      await self.__cluster_monitor.update()
    return await self.__cluster_db.get_cluster()

  async def find_hosts(self, ttl=30, **kwargs):
    if self._is_cache_expired(ttl):
      await self.__cluster_monitor.update()
    return await self.__cluster_db.find_hosts(**kwargs)

  def start_monitor(self):
    self.__cluster_monitor.start()

  def _is_cache_expired(self, ttl):
    if ttl is None:
      return False
    if not self.__cluster_monitor.last_update:
      return True
    ttl = datetime.timedelta(seconds=ttl)
    return self.__cluster_monitor.last_update - datetime.datetime.now(tz=None) > ttl


class ClusterMonitor(AutonomousMonitor):
  
  def __init__(self, api, db, interval=30000):
    super(ClusterMonitor, self).__init__(interval)
    self.__api = api
    self.__db = db
    self.__last_update = None

  @property
  def last_update(self):
    return self.__last_update

  async def update(self):
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
    self.__last_update = datetime.datetime.now(tz=None)
  
  async def callback(self):
    await self.update()


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


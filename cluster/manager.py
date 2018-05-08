from tornado.ioloop import PeriodicCallback

from commons import APIManager, Singleton, MotorClient, Loggable, AsyncHttpClientWrapper
from cluster.base import Host, HostResources
from config import config


class ClusterManager(Loggable, metaclass=Singleton):

  MONITOR_INTERVAL = 30000 # query cluster info every minute

  def __init__(self):
    self.__cluster_db = ClusterDBManager()
    self.__http_cli = AsyncHttpClientWrapper()

  async def get_cluster(self):
    return self.__cluster_db.get_cluster()

  async def find_hosts(self, **kwargs):
    return self.__cluster_db.find_hosts(**kwargs)

  async def monitor(self):

    async def get_updated_master():
      marathon_api, mesos_api = config.marathon, config.mesos
      status, body, err = await self.__http_cli.get(marathon_api.host, marathon_api.port,
                                                    '%s/leader'%marathon_api.endpoint)
      if status != 200:
        self.logger.error(err)
        raise Exception('Failed to get leader master')
      host, port = body.get('leader', ':').split(':')
      if host != mesos_api.host:
        mesos_api.host = host
        marathon_api.host, marathon_api.port = host, port
        self.logger.info('Current leader: %s:%d'%(mesos_api.host, mesos_api.port))

    async def query_mesos():
      await get_updated_master()
      api = config.mesos
      status, body, err = await self.__http_cli.get(api.host, api.port,
                                                    '%s/master/slaves'%api.endpoint)
      if status != 200:
        print(status, body)
        self.logger.debug(err)
        return
      self.logger.debug('Collect host info')
      hosts = [Host(hostname=h['hostname'],
                    resources=HostResources(h['resources']['cpus'],
                                            h['resources']['mem'],
                                            h['resources']['disk'],
                                            h['resources']['gpus'],
                                            h['resources']['ports'][1:-1].split(',')),
                    attributes=h['attributes'])
               for h in body['slaves']]
      await self.__cluster_db.update_hosts(hosts)

    await query_mesos()
    PeriodicCallback(query_mesos, self.MONITOR_INTERVAL).start()


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


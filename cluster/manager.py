from tornado.ioloop import PeriodicCallback

from cluster.base import Host, HostResources
from util import Singleton, MotorClient, Loggable, SecureAsyncHttpClient


class ClusterManager(Loggable, metaclass=Singleton):

  MONITOR_INTERVAL = 30000 # query cluster info every minute

  def __init__(self, config):
    self.__config = config
    self.__host_col = MotorClient().requester.host
    self.__http_cli = SecureAsyncHttpClient(config)

  async def get_cluster(self):
    return [Host(hostname=h['attributes']['hostname'],
                 resources=HostResources(**h['resources']),
                 attributes=h['attributes']) async for h in self.__host_col.find()]

  async def find_hosts_by_attribute(self, k, v):
    return [Host(**h) async for h in self.__host_col.find({k: v})]

  async def update_hosts(self, hosts):
    for h in hosts:
      await self.__host_col.replace_one(dict(hostname=h.hostname), h.to_save(),
                                        upsert=True)

  async def monitor(self):
    async def query_mesos():
      mesos_master = self.__config.url.mesos_master
      status, body, err = await self.__http_cli.get('%s/slaves'%mesos_master)
      if status != 200:
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
      await self.update_hosts(hosts)

    await query_mesos()
    PeriodicCallback(query_mesos, self.MONITOR_INTERVAL).start()

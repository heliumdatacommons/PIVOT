from tornado.ioloop import PeriodicCallback

from cluster.base import Cluster, Host, Resources
from util import Singleton, MotorClient, Loggable, SecureAsyncHttpClient


class ClusterManager(Loggable, metaclass=Singleton):

  MONITOR_INTERVAL = 30000 # query cluster info every minute

  def __init__(self, config):
    self.__config = config
    self.__cluster_col = MotorClient().requester.cluster
    self.__http_cli = SecureAsyncHttpClient(config)

  async def get_cluster(self):
    c = await self.__cluster_col.find_one(dict(id=self.__config.url.mesos_master))
    return Cluster(id=c['id'],
                   hosts=[Host(hostname=h['attributes']['hostname'],
                               resources=Resources(**h['resources']),
                               attributes=h['attributes'])
                          for h in c['hosts']])

  async def find_hosts_by_attribute(self, k, v):
    cluster = self.get_cluster()
    return [h for h in cluster.hosts if h.attributes.get(k, None) == v]

  async def update_cluster(self, cluster):
    await self.__cluster_col.replace_one(dict(id=cluster.id), cluster.to_save(),
                                         upsert=True)

  async def monitor(self):
    async def query_mesos():
      mesos_master = self.__config.url.mesos_master
      status, body, err = await self.__http_cli.get('%s/slaves'%mesos_master)
      if status != 200:
        self.logger.debug(err)
        return
      self.logger.debug('Collect host info')
      cluster = Cluster(id=mesos_master,
                        hosts=[Host(
                           hostname=h['hostname'],
                           resources=Resources(h['resources']['cpus'],
                                               h['resources']['mem'],
                                               h['resources']['disk'],
                                               h['resources']['gpus'],
                                               h['resources']['ports'][1:-1].split(',')),
                           attributes=h['attributes'])
                      for h in body['slaves']])
      await self.update_cluster(cluster)

    await query_mesos()
    PeriodicCallback(query_mesos, self.MONITOR_INTERVAL).start()

from collections import defaultdict
from tornado.ioloop import PeriodicCallback

from util import Singleton, Loggable, SecureAsyncHttpClient


class Host:

  def __init__(self, hostname, attributes={}):
    self.__attributes = dict(**attributes, hostname=hostname)

  @property
  def hostname(self):
    return self.__attributes.get('hostname', None)

  @property
  def attributes(self):
    return dict(self.__attributes)

  def to_render(self):
    return self.attributes


class Cluster(Loggable, metaclass=Singleton):

  MONITOR_INTERVAL = 30000 # query cluster info every minute

  def __init__(self, config):
    self.__config = config
    self.__http_cli = SecureAsyncHttpClient(config)
    self.__hosts = []
    self.__attributes_map = defaultdict(set)

  @property
  def hosts(self):
    return list(self.__hosts)

  def find_host_by_attribute(self, key, val):
    return list(self.__attributes_map.get((key, val), []))

  async def monitor(self):
    async def query_mesos():
      status, body, err = await self.__http_cli.get('%s/slaves'%self.__config.url.mesos_master)
      if status != 200:
        self.logger.debug(err)
        return
      self.logger.debug('Collect host info')
      self.__hosts = [Host(hostname=h['hostname'], attributes=h['attributes'])
                      for h in body['slaves']]
      for h in self.__hosts:
        for kv_pair in h.attributes.items():
          self.__attributes_map[kv_pair].add(h)

    await query_mesos()
    PeriodicCallback(query_mesos, self.MONITOR_INTERVAL).start()



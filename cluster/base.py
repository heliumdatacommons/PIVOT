import bisect

from collections import defaultdict
from tornado.ioloop import PeriodicCallback

from util import Singleton, Loggable, SecureAsyncHttpClient


class Host:

  def __init__(self, hostname, resources, attributes={}):
    self.__resources = resources
    self.__attributes = dict(**attributes, hostname=hostname)

  @property
  def hostname(self):
    return self.__attributes.get('hostname', None)

  @property
  def resources(self):
    return self.__resources

  @property
  def attributes(self):
    return dict(self.__attributes)

  def to_render(self):
    return dict(attributes=self.attributes, resources=self.resources.to_render())


class Resources:

  def __init__(self, cpus, mem, disk, gpus, port_ranges):
    self.__cpus = cpus
    self.__mem = mem
    self.__disk = disk
    self.__gpus = gpus
    self.__port_ranges = [tuple(map(lambda p: int(p), p.split('-'))) for p in port_ranges]

  @property
  def cpus(self):
    return self.__cpus

  @property
  def mem(self):
    return self.__mem

  @property
  def disk(self):
    return self.__disk

  @property
  def gpus(self):
    return self.__gpus

  @property
  def port_ranges(self):
    return self.__port_ranges

  def check_port_availability(self, p):
    assert isinstance(p, int)
    starts = [p[0] for p in self.port_ranges]
    idx = bisect.bisect(starts, p, 0, len(starts))
    if idx == 0:
      return False
    port_range = self.port_ranges[idx - 1]
    return port_range[0] <= p <= port_range[1]

  def to_render(self):
    return dict(cpus=self.cpus, mem=self.mem, disk=self.disk, gpus=self.gpus,
                port_ranges=['%d-%d'%p for p in self.port_ranges])


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
      self.__hosts = [Host(hostname=h['hostname'],
                           resources=Resources(h['resources']['cpus'],
                                               h['resources']['mem'],
                                               h['resources']['disk'],
                                               h['resources']['gpus'],
                                               h['resources']['ports'][1:-1].split(',')),
                           attributes=h['attributes'])
                      for h in body['slaves']]
      for h in self.__hosts:
        for kv_pair in h.attributes.items():
          self.__attributes_map[kv_pair].add(h)

    await query_mesos()
    PeriodicCallback(query_mesos, self.MONITOR_INTERVAL).start()



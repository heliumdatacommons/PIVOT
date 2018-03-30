import bisect

import swagger


class Host:

  def __init__(self, hostname, resources, attributes={}):
    self.__resources = resources
    self.__attributes = dict(**attributes)
    self.__attributes.update(hostname=hostname)

  @property
  def hostname(self):
    return self.__attributes.get('hostname', None)

  @property
  def resources(self):
    """
    :type: {cluster.base.Resources}
    """
    return self.__resources

  @property
  def attributes(self):
    return dict(self.__attributes)

  def to_render(self):
    return dict(attributes=self.attributes, resources=self.resources.to_render())

  def to_save(self):
    return self.to_render()


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

  def to_save(self):
    return self.to_render()


class Cluster:

  def __init__(self, id, hosts=[]):
    self.__id = id
    self.__hosts = hosts

  @property
  def id(self):
    return self.__id

  @property
  def hosts(self):
    return list(self.__hosts)

  def to_render(self):
    return dict(hosts=[h.to_render() for h in self.hosts])

  def to_save(self):
    return dict(id=self.id, hosts=[h.to_save() for h in self.hosts])





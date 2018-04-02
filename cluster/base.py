import bisect

import swagger


@swagger.model
class Host:
  """
  Physical host in the cluster

  """

  def __init__(self, hostname, resources, attributes={}, *args, **kwargs):
    self.__resources = resources
    self.__attributes = dict(**attributes)
    self.__attributes.update(hostname=hostname)

  @property
  @swagger.property
  def hostname(self):
    """
    Hostname

    ---
    type: str
    read_only: true
    example: 10.52.0.1

    """
    return self.__attributes.get('hostname', None)

  @property
  @swagger.property
  def resources(self):
    """
    Resources available on the host
    ---
    type: HostResources
    read_only: true
    """
    return self.__resources

  @property
  @swagger.property
  def attributes(self):
    """
    Key-value attribute pairs associated with the host

    ---
    type: dict
    read_only: true
    additional_properties:
      type: str
    example:
      region: us-east1
    """
    return dict(self.__attributes)

  def to_render(self):
    return dict(hostname=self.hostname,
                attributes=self.attributes,
                resources=self.resources.to_render())

  def to_save(self):
    return self.to_render()


@swagger.model
class HostResources:
  """
  Resources on a physical host

  """

  def __init__(self, cpus, mem, disk, gpus, port_ranges):
    self.__cpus = cpus
    self.__mem = mem
    self.__disk = disk
    self.__gpus = gpus
    self.__port_ranges = [tuple(map(lambda p: int(p), p.split('-'))) for p in port_ranges]

  @property
  @swagger.property
  def cpus(self):
    """
    Number of CPU cores
    ---
    type: int
    minimum: 1
    read_only: true
    example: 2

    """
    return self.__cpus

  @property
  @swagger.property
  def mem(self):
    """
    Memory in MB
    ---
    type: int
    read_only: true
    example: 4096

    """
    return self.__mem

  @property
  @swagger.property
  def disk(self):
    """
    Disk space in MB
    ---
    type: int
    read_only: true
    example: 10240

    """
    return self.__disk

  @property
  @swagger.property
  def gpus(self):
    """
    Number of GPU units
    ---
    type: int
    read_only: true
    example: 1

    """
    return self.__gpus

  @property
  @swagger.property
  def port_ranges(self):
    """
    Available port ranges
    ---
    type: list
    items: str
    read_only: true
    example:
      - 1025-2180
      - 2182-3887
      - 3889-5049
      - 5052-8079
      - 8082-8180
      - 8182-32000

    """
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





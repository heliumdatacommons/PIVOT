from enum import Enum


class ContainerType(Enum):

  SERVICE = 'service'
  JOB = 'job'


class ContainerState(Enum):

  SUBMITTED = 'submitted'
  PENDING = 'pending'
  STAGING = 'staging'
  RUNNING = 'running'
  SUCCESS = 'success'
  FAILED = 'failed'

  @classmethod
  def has_value(cls, val):
    return any(val == item.value for item in cls)

  @classmethod
  def determine_state(cls, states, minimum_capacity=1.):
    """

    :param states: MESOS states
    :param minimum_capacity:
    :return:
    """
    if not states:
      return ContainerState.SUBMITTED
    running_tasks = sum([1 for s in states if s == 'TASK_RUNNING'])
    staging_tasks = sum([1 for s in states if s == 'TASK_STAGING'])
    starting_tasks = sum([1 for s in states if s == 'TASK_STARTING'])
    if staging_tasks:
      return ContainerState.STAGING
    if starting_tasks:
      return ContainerState.PENDING
    if running_tasks/len(states) >= minimum_capacity:
      return ContainerState.RUNNING
    return ContainerState.FAILED


class NetworkMode(Enum):

  HOST = 'HOST'
  BRIDGE = 'BRIDGE'
  CONTAINER = 'CONTAINER'

  @classmethod
  def has_value(cls, val):
    return any(val == item.value for item in cls)


class Volume:

  def __init__(self, container_path, host_path, mode):
    self.__container_path = container_path
    self.__host_path = host_path
    self.__mode = mode

  @property
  def container_path(self):
    return self.__container_path

  @property
  def host_path(self):
    return self.__host_path

  @property
  def mode(self):
    return self.__mode

  def to_render(self):
    return dict(container_path=self.container_path,
                host_path=self.host_path,
                mode=self.mode)

  def to_save(self):
    return self.to_render()

  def to_request(self):
    return dict(containerPath=self.__container_path,
                hostPath=self.container_path,
                mode=self.mode)


class Endpoint:

  def __init__(self, host, container_port, host_port, protocol='tcp'):
    self.__host = host
    self.__host_port = host_port
    self.__container_port = container_port
    self.__protocol = protocol

  @property
  def host(self):
    return self.__host

  @property
  def host_port(self):
    return self.__host_port

  @property
  def container_port(self):
    return self.__container_port

  @property
  def protocol(self):
    return self.__protocol

  def to_render(self):
    return dict(host=self.__host, host_port=self.__host_port,
                container_port=self.__container_port, protocol=self.__protocol)

  def to_save(self):
    return self.to_render()

  def __repr__(self):
    return str(self.to_render())


class Port:

  def __init__(self, container_port, host_port=0, load_balanced_port=None,
               protocol='tcp'):
    self.__container_port = container_port
    self.__host_port = host_port
    self.__protocol = protocol
    self.__load_balanced_port = load_balanced_port or container_port

  @property
  def container_port(self):
    return self.__container_port

  @property
  def host_port(self):
    return self.__host_port

  @property
  def protocol(self):
    return self.__protocol

  @property
  def load_balanced_port(self):
    return self.__load_balanced_port

  def to_render(self):
    return dict(container_port=self.container_port, host_port=self.host_port,
                load_balanced_port=self.load_balanced_port, protocol=self.protocol)

  def to_save(self):
    return self.to_render()

  def __eq__(self, other):
    return self.__container_port == other.container_port \
             and self.__host_port == other.host_port \
             and self.__protocol == other.protocol

  def __hash__(self):
    return hash(self.__container_port) ^ hash(self.__host_port) ^ hash(self.__protocol)

  def __repr__(self):
    return str(self.to_render())


class Resources:

  def __init__(self, cpus, mem, disk=0, gpu=0):
    self.__cpus = cpus
    self.__mem = mem
    self.__disk = disk
    self.__gpu = gpu

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
  def gpu(self):
    return self.__gpu

  def to_render(self):
    return dict(cpus=self.cpus, mem=self.mem,
                disk=self.disk, gpu=self.gpu)

  def to_save(self):
    return self.to_render()

  def to_request(self):
    return self.to_render()


class Container:

  REQUIRED=frozenset(['id', 'type', 'image', 'resources'])

  def __init__(self, id, appliance, type, image, resources, cmd=None, args=[], env={},
               volumes=[], network_mode=NetworkMode.HOST, endpoints=[], ports=[],
               state=ContainerState.SUBMITTED, is_privileged=False, force_pull_image=True,
               dependencies=[], rack=None, host=None, last_update=None, **kwargs):
    self.__id = id
    self.__appliance = appliance
    self.__type = type if isinstance(type, ContainerType) else ContainerType(type)
    self.__image = image
    self.__resources = Resources(**resources)
    self.__cmd = cmd
    self.__args = list(args)
    if self.__cmd and self.__args:
      raise ValueError("Cannot specify both 'cmd' and 'args'")
    self.__env = dict(env)
    self.__volumes = [Volume(**v) for v in volumes]
    self.__network_mode = network_mode if isinstance(network_mode, NetworkMode) \
                      else NetworkMode(network_mode.upper())
    self.__endpoints = [Endpoint(**e) for e in endpoints]
    self.__ports = [Port(**p) for p in ports]
    self.__state = state if isinstance(state, ContainerState) else ContainerState(state)
    self.__rack = rack
    self.__host = host
    self.__is_privileged = is_privileged
    self.__force_pull_image = force_pull_image
    self.__dependencies = dependencies
    self.__last_update = last_update

  @classmethod
  def pre_check(cls, data):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse container data format: %s"%type(data)
    missing = Container.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of container: %s"%missing
    return 200, "Container %s is valid"%data['id'], None

  @property
  def id(self):
    return self.__id

  @property
  def appliance(self):
    return self.__appliance

  @property
  def type(self):
    return self.__type

  @property
  def image(self):
    return self.__image

  @property
  def resources(self):
    return self.__resources

  @property
  def cmd(self):
    return self.__cmd

  @property
  def args(self):
    return list(self.__args)

  @property
  def env(self):
    return dict(self.__env)

  @property
  def volumes(self):
    return list(self.__volumes)

  @property
  def network_mode(self):
    return self.__network_mode

  @property
  def endpoints(self):
    return list(self.__endpoints)

  @property
  def ports(self):
    return list(self.__ports)

  @property
  def state(self):
    return self.__state

  @property
  def rack(self):
    return self.__rack

  @property
  def host(self):
    return self.__host

  @property
  def is_privileged(self):
    return self.__is_privileged

  @property
  def force_pull_image(self):
    return self.__force_pull_image

  @property
  def dependencies(self):
    return list(self.__dependencies)

  @property
  def last_update(self):
    return self.__last_update

  @image.setter
  def image(self, image):
    assert isinstance(image, str)
    self.__image = image

  @resources.setter
  def resources(self, resources):
    self.__resources = resources

  @appliance.setter
  def appliance(self, app):
    assert isinstance(app, str)
    self.__appliance = app

  @endpoints.setter
  def endpoints(self, endpoints):
    self.__endpoints = list(endpoints)

  @state.setter
  def state(self, state):
    self.__state = state if isinstance(state, ContainerState) else ContainerState(state)

  @rack.setter
  def rack(self, rack):
    self.__rack = rack

  @host.setter
  def host(self, host):
    self.__host = host

  def add_env(self, **env):
    self.__env.update(env)

  def add_dependency(self, dep):
    self.__dependencies.append(dep)

  def to_render(self):
    return dict(id=self.id, appliance=self.appliance, type=self.type.value,
                image=self.image, resources=self.resources.to_render(),
                cmd=self.cmd, args=self.args, env=self.env,
                volumes=[v.to_render() for v in self.volumes],
                network_mode=self.network_mode.value,
                endpoints=[e.to_render() for e in self.endpoints],
                ports=[p.to_render() for p in self.ports],
                state=self.state.value, is_privileged=self.is_privileged,
                force_pull_image=self.force_pull_image, dependencies=self.dependencies,
                rack=self.rack, host=self.host, last_update=self.last_update)

  def to_save(self):
    return dict(id=self.id, appliance=self.appliance, type=self.type.value,
                image=self.image, resources=self.resources.to_save(),
                cmd=self.cmd, args=self.args, env=self.env,
                volumes=[v.to_save() for v in self.volumes],
                network_mode=self.network_mode.value,
                endpoints=[e.to_save() for e in self.endpoints],
                ports=[p.to_save() for p in self.ports],
                state=self.state.value, is_privileged=self.is_privileged,
                force_pull_image=self.force_pull_image, dependencies=self.dependencies,
                rack=self.rack, host=self.host, last_update=self.last_update)

import re
import swagger

from enum import Enum

from util import parse_datetime


@swagger.enum
class ContainerType(Enum):
  """
  "service" for long-running containers, "job" for one-off containers

  """

  SERVICE = 'service'
  JOB = 'job'


@swagger.enum
class ContainerState(Enum):
  """
  Container state

  """
  SUBMITTED = 'submitted'
  PENDING = 'pending'
  STAGING = 'staging'
  RUNNING = 'running'
  SUCCESS = 'success'
  FAILED = 'failed'
  KILLED = 'killed'

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


@swagger.enum
class NetworkMode(Enum):
  """
  Container network mode

  """

  HOST = 'HOST'
  BRIDGE = 'BRIDGE'
  CONTAINER = 'CONTAINER'

  @classmethod
  def has_value(cls, val):
    return any(val == item.value for item in cls)


@swagger.model
class Volume:
  """
  Volume mounted to the container. Only support host local volumes currently

  """

  def __init__(self, container_path, host_path, mode, *args, **kwargs):
    self.__container_path = container_path
    self.__host_path = host_path
    self.__mode = mode

  @property
  @swagger.property
  def container_path(self):
    """
    Volume mount point in the container
    ---
    type: str
    required: true
    example: /mnt/data

    """
    return self.__container_path

  @property
  @swagger.property
  def host_path(self):
    """
    Physical path of the volume on the host
    ---
    type: str
    required: true
    example: /home/user/data

    """
    return self.__host_path

  @property
  @swagger.property
  def mode(self):
    """
    Read-write mode
    ---
    type: str
    required: true
    example: RW

    """
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


@swagger.model
class Endpoint:
  """
  Endpoint for accessing the container

  """

  def __init__(self, host, container_port, host_port, protocol='tcp', *args, **kwargs):
    self.__host = host
    self.__host_port = host_port
    self.__container_port = container_port
    self.__protocol = protocol

  @property
  @swagger.property
  def host(self):
    """
    Public IP address/hostname of the host where the container is running
    ---
    type: str
    example: 192.168.1.1

    """
    return self.__host

  @property
  @swagger.property
  def host_port(self):
    """
    The port number on the host mapped to the container's listening port, which is used
    for accessing the container
    ---
    type: int
    maximum: 65535
    minimum: 1
    example: 18080

    """
    return self.__host_port

  @property
  @swagger.property
  def container_port(self):
    """
    The container's listening port
    ---
    type: int
    maximum: 65535
    minimum: 1
    example: 8080

    """
    return self.__container_port

  @property
  @swagger.property
  def protocol(self):
    """
    The transport protocol for communication
    ---
    type: str
    default: tcp
    example: tcp

    """
    return self.__protocol

  def to_render(self):
    return dict(host=self.__host, host_port=self.__host_port,
                container_port=self.__container_port, protocol=self.__protocol)

  def to_save(self):
    return self.to_render()

  def __repr__(self):
    return str(self.to_render())


@swagger.model
class Port:
  """
  Container port definition

  """

  def __init__(self, container_port, host_port=0, protocol='tcp', *args, **kwargs):
    self.__container_port = container_port
    self.__host_port = host_port
    self.__protocol = protocol

  @property
  @swagger.property
  def container_port(self):
    """
    Port number the container listens to
    ---
    type: int
    required: true
    maximum: 65535
    minimum: 1
    example: 8080

    """
    return self.__container_port

  @property
  @swagger.property
  def host_port(self):
    """
    Host port number the container listening port is mapped to. Random port number will be
    assigned if set 0
    ---
    type: int
    maximum: 65535
    minimum: 0
    example: 18080
    default: 0

    """
    return self.__host_port

  @property
  @swagger.property
  def protocol(self):
    """
    The transport protocol for communication
    ---
    type: str
    default: tcp
    example: tcp

    """
    return self.__protocol

  def to_render(self):
    return dict(container_port=self.container_port, host_port=self.host_port,
                protocol=self.protocol)

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


@swagger.model
class Resources:
  """
  Resources specifications

  """

  def __init__(self, cpus, mem, disk=0, gpu=0, *args, **kwargs):
    self.__cpus = cpus
    self.__mem = mem
    self.__disk = disk
    self.__gpu = gpu

  @property
  @swagger.property
  def cpus(self):
    """
    Number of CPU cores
    ---
    type: int
    required: true
    minimum: 1
    example: 1

    """
    return self.__cpus

  @property
  @swagger.property
  def mem(self):
    """
    Memory size in MB
    ---
    type: int
    required: true
    minimum: 128
    example: 2048

    """
    return self.__mem

  @property
  @swagger.property
  def disk(self):
    """
    Disk size in MB
    ---
    type: int
    default: 0
    example: 10240

    """
    return self.__disk

  @property
  @swagger.property
  def gpu(self):
    """
    Number of GPU units
    ---
    type: int
    default: 0
    example: 1

    """
    return self.__gpu

  def to_render(self):
    return dict(cpus=self.cpus, mem=self.mem,
                disk=self.disk, gpu=self.gpu)

  def to_save(self):
    return self.to_render()

  def to_request(self):
    return self.to_render()


@swagger.model
class Data:
  """
  Data specifications

  """
  def __init__(self, input=[], *args, **kwargs):
    self.__input = list(input)

  @property
  @swagger.property
  def input(self):
    """
    Paths of input data objects
    ---
    type: list
    items: str
    default: []
    example:
      - /tempZone/rods/a.file
      - /tempZone/rods/b.file

    """
    return list(self.__input)

  def to_render(self):
    return dict(input=self.input)

  def to_save(self):
    return self.to_render()


@swagger.model
class Container:
  """
  Container specifications

  """

  REQUIRED = frozenset(['id', 'type', 'image', 'resources'])
  ID_PATTERN = r'[a-zA-Z0-9-]+'

  @classmethod
  def parse(cls, data):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse container data format: %s"%type(data)
    missing = Container.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of container: %s"%missing
    if data['type'] == ContainerType.SERVICE.value:
      from container.service import Service
      return 200, Service(**data), None
    if data['type'] == ContainerType.JOB.value:
      from container.job import Job
      try:
        return 200, Job(**data), None
      except ValueError as e:
        return 400, None, str(e)
    return 422, 'Unknown container type: %s'%data['type'], None

  def __init__(self, id, appliance, type, image, resources, cmd=None, args=[], env={},
               volumes=[], network_mode=NetworkMode.HOST, endpoints=[], ports=[],
               state=ContainerState.SUBMITTED, is_privileged=False, force_pull_image=True,
               dependencies=[], data=None, cloud=None, host=None, last_update=None,
               schedule=None, deployment=None, *aargs, **kwargs):
    self.__id = id
    self.__appliance = appliance
    self.__type = type if isinstance(type, ContainerType) else ContainerType(type)
    self.__image = image
    self.__resources = Resources(**resources)
    self.__cmd = cmd and str(cmd)
    self.__args = [a and str(a) for a in args]
    if self.__cmd and self.__args:
      raise ValueError("Cannot specify both 'cmd' and 'args'")
    self.__env = {k: v and str(v) for k, v in env.items()}
    self.__volumes = [Volume(**v) for v in volumes]
    self.__network_mode = network_mode if isinstance(network_mode, NetworkMode) \
                      else NetworkMode(network_mode.upper())
    self.__endpoints = [Endpoint(**e) for e in endpoints]
    self.__ports = [Port(**p) for p in ports]
    self.__state = state if isinstance(state, ContainerState) else ContainerState(state)
    self.__cloud = cloud
    self.__host = host
    self.__is_privileged = is_privileged
    self.__force_pull_image = force_pull_image
    self.__dependencies = list(dependencies)
    self.__data = data and Data(**data)
    self.__last_update = parse_datetime(last_update)
    self.__schedule = Schedule(**(schedule if schedule else {}))
    self.__deployment = deployment and Deployment(**deployment)

  @property
  @swagger.property
  def id(self):
    """
    Unique identifier of the container in an appliance
    ---
    type: str
    required: true
    example: test-container

    """
    return self.__id

  @property
  @swagger.property
  def appliance(self):
    """
    The appliance in which the container is running
    ---
    type: str
    example: test-app
    read_only: true

    """
    return self.__appliance

  @property
  @swagger.property
  def type(self):
    """
    Container type
    ---
    type: ContainerType
    required: true
    example: service

    """
    return self.__type

  @property
  @swagger.property
  def image(self):
    """
    Container image ID. The image is either locally available in the cluster or publicly
    hosted on Docker Hub
    ---
    type: str
    required: true
    example: centos:7

    """
    return self.__image

  @property
  @swagger.property
  def resources(self):
    """
    Resources allocated to the container
    ---
    type: Resources
    required: true
    example: Resources

    """
    return self.__resources

  @property
  @swagger.property
  def cmd(self):
    """
    Command to be run on the container. Mutually exclusive to `args` property
    ---
    type: str
    nullable: true
    example: /bin/bash

    """
    return self.__cmd

  @property
  @swagger.property
  def args(self):
    """
    A list of arguments for running the container. Mutually exclusive to `cmd` property
    ---
    type: list
    items: str
    default: []
    example:
      - --data_dir
      - /home/user/data
    """
    return list(self.__args)

  @property
  @swagger.property
  def env(self):
    """
    Environment variables set within the container
    ---
    type: object
    additional_properties:
      type: str
    default: {}
    example:
      DATA_DIR: /home/user/data
    """
    return dict(self.__env)

  @property
  @swagger.property
  def volumes(self):
    """
    Volumes to be mounted to the container
    ---
    type: list
    items: Volume
    default: []

    """
    return list(self.__volumes)

  @property
  @swagger.property
  def network_mode(self):
    """
    Network mode of the container
    ---
    type: NetworkMode
    required: true
    default: host
    example: container

    """
    return self.__network_mode

  @property
  @swagger.property
  def endpoints(self):
    """
    Endpoints for accessing the container
    ---
    type: list
    items: Endpoint
    default: []
    read_only: true

    """
    return list(self.__endpoints)

  @property
  @swagger.property
  def ports(self):
    """
    Port definitions of the container
    ---
    type: list
    items: Port
    default: []

    """
    return list(self.__ports)

  @property
  @swagger.property
  def state(self):
    """
    Container state
    ---
    type: ContainerState
    read_only: true

    """
    return self.__state

  @property
  @swagger.property
  def cloud(self):
    """
    Placement constraint: Cloud platform where the container must be placed
    ---
    type: str
    nullable: true
    example: 'aws'

    """
    return self.__cloud

  @property
  @swagger.property
  def host(self):
    """
    Placement constraint: physical host identified by the public IP address where the
    container must be placed on
    ---
    type: str
    nullable: true
    example: '35.23.5.16'

    """
    return self.__host


  @property
  @swagger.property
  def is_privileged(self):
    """
    Whether to run the container in `privileged` mode
    ---
    type: bool
    required: true
    default: false
    example: false

    """
    return self.__is_privileged

  @property
  @swagger.property
  def force_pull_image(self):
    """
    Whether to pull the latest container image from the image repository (e.g., Docker
    Hub) when launching the container
    ---
    type: bool
    required: true
    default: true
    example: true

    """
    return self.__force_pull_image

  @property
  @swagger.property
  def dependencies(self):
    """
    Other containers in the appliance that the container depends on
    ---
    type: list
    items: str
    default: []
    example:
      - c1
      - j2
    """
    return list(self.__dependencies)

  @property
  @swagger.property
  def data(self):
    """
    Data consumed by the container
    ---
    type: Data

    """
    return self.__data

  @property
  def last_update(self):
    return self.__last_update

  @property
  def schedule(self):
    return self.__schedule

  @property
  @swagger.property
  def deployment(self):
    """
    Container deployment info
    ---
    type: Deployment
    read_only: true

    """
    return self.__deployment

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

  @last_update.setter
  def last_update(self, last_update):
    self.__last_update = parse_datetime(last_update)

  @deployment.setter
  def deployment(self, deployment):
    self.__deployment = deployment

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
                data=self.data and self.data.to_render(),
                cloud=self.cloud, host=self.host,
                deployment=self.deployment and self.deployment.to_render())

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
                data=self.data and self.data.to_save(),
                cloud=self.cloud, host=self.host,
                last_update=self.last_update and self.last_update.isoformat(),
                schedule=self.schedule and self.schedule.to_save(),
                deployment=self.deployment and self.deployment.to_save())

  def __hash__(self):
    return hash((self.id, self.appliance))

  def __eq__(self, other):
    return self.__class__ == other.__class__ \
            and self.id == other.id \
            and self.appliance == other.appliance


################################
### Internal data structures ###
################################

class Schedule:

  def __init__(self, constraints={}, *args, **kwargs):
    self.__constraints = dict(constraints)

  @property
  def constraints(self):
    return dict(self.__constraints)

  def add_constraint(self, key, value):
    self.__constraints[key] = value

  def to_save(self):
    return dict(constraints=self.constraints)


@swagger.model
class Deployment:

  def __init__(self, ip_addresses=[], cloud=None, host=None):
    self.__ip_addresses = list(ip_addresses)
    self.__cloud = cloud
    self.__host = host

  @property
  @swagger.property
  def cloud(self):
    """
    Cloud platform where the container is deployed
    ---
    type: str
    read_only: true

    """
    return self.__cloud

  @property
  @swagger.property
  def host(self):
    """
    Physical host where the container is deployed
    ---
    type: str
    read_only: true

    """
    return self.__host

  @property
  def ip_addresses(self):
    return list(self.__ip_addresses)

  @cloud.setter
  def cloud(self, cloud):
    self.__cloud = cloud

  @host.setter
  def host(self, host):
    self.__host = host

  def add_ip_address(self, ip_addr):
    self.__ip_addresses.append(ip_addr)

  def to_render(self):
    return dict(cloud=self.cloud, host=self.host)

  def to_save(self):
    return dict(ip_addresses=self.ip_addresses, cloud=self.cloud, host=self.host)


short_id_pattern = r'@(%s)'%Container.ID_PATTERN


def get_short_ids(p):
  return re.compile(short_id_pattern).findall(p) if p else []


def parse_container_short_id(p, appliance):
  return re.sub(r'([^@]*)%s([^@]*)'%short_id_pattern,
                r'\1\2-%s.marathon.containerip.dcos.thisdcos.directory\3'%appliance,
                str(p))


import re
import json
import swagger

import appliance
import schedule

from enum import Enum

from util import parse_datetime
from locality import Placement


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


@swagger.enum
class ContainerVolumeType(Enum):

  HOST = 'HOST'
  PERSISTENT = 'PERSISTENT'


@swagger.model
class ContainerVolume:
  """
  Volume mounted to the container.

  """

  def __init__(self, src, dest, type=ContainerVolumeType.PERSISTENT, *args, **kwargs):
    self.__src = src
    self.__dest = dest
    self.__type = type if isinstance(type, ContainerVolumeType) else ContainerVolumeType(type.upper())

  @property
  @swagger.property
  def src(self):
    """
    Source of the volume
    ---
    type: str
    required: true
    example: /home/user/data

    """
    return self.__src

  @property
  @swagger.property
  def dest(self):
    """
    Mountpoint of the volume in the container
    ---
    type: str
    required: true
    example: /mnt/data

    """
    return self.__dest

  @property
  @swagger.property
  def type(self):
    """
    Volume type
    ---
    type: ContainerVolumeType
    example: persistent

    """
    return self.__type

  def to_render(self):
    return dict(src=self.src, dest=self.dest, type=self.type.value)

  def to_save(self):
    return self.to_render()


@swagger.model
class Endpoint:
  """
  Endpoint for accessing the container

  """

  def __init__(self, host, container_port, host_port, protocol='tcp', name=None, *args, **kwargs):
    self.__host = host
    self.__host_port = host_port
    self.__container_port = container_port
    self.__protocol = protocol
    self.__name = name

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

  @property
  @swagger.property
  def name(self):
    """
    The descriptive name of the endpoint
    ---
    type: str
    example: Jupyter Notebook
    """
    return self.__name

  @name.setter
  def name(self, name):
    self.__name = name

  def to_render(self):
    return dict(host=self.__host, host_port=self.__host_port,
                container_port=self.__container_port, protocol=self.__protocol, name=self.__name)

  def to_save(self):
    return self.to_render()

  def __repr__(self):
    return str(self.to_render())


@swagger.model
class Port:
  """
  Container port definition

  """

  def __init__(self, container_port, host_port=0, protocol='tcp', name=None, *args, **kwargs):
    self.__container_port = container_port
    self.__host_port = host_port
    self.__protocol = protocol
    self.__name = name

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

  @property
  @swagger.property
  def name(self):
    return self.__name

  def to_render(self):
    return dict(container_port=self.container_port, host_port=self.host_port,
                protocol=self.protocol, name=self.name)

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
class ContainerScheduleHints(schedule.ScheduleHints):

  @classmethod
  def parse(self, data, from_user=True):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse container data format: %s" % type(data)
    return 200, ContainerScheduleHints(**data), None

  def __init__(self, preemptible=False, *args, **kwargs):
    super(ContainerScheduleHints, self).__init__(*args, **kwargs)
    self.__preemptible = preemptible

  @property
  @swagger.property
  def preemptible(self):
    """
    Whether the container is preemptible
    ---
    type: bool
    default: False

    """
    return self.__preemptible

  def to_render(self):
    return dict(**super(ContainerScheduleHints, self).to_render(), preemptible=self.preemptible)

  def to_save(self):
    return dict(**super(ContainerScheduleHints, self).to_save(), preemptible=self.preemptible)


@swagger.model
class Container:
  """
  Container specifications

  """

  REQUIRED = frozenset(['id', 'type', 'image', 'resources'])
  ID_PATTERN = r'[a-zA-Z0-9-]+'

  @classmethod
  def parse(cls, data, from_user=True):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse container data format: %s"%type(data)
    missing = Container.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of container: %s"%missing
    if from_user:
      for unwanted_f in ('deployment', ):
        data.pop(unwanted_f, None)
    if from_user:
      sched_hints = data.get('schedule_hints')
      if sched_hints:
        status, sched_hints, err = ContainerScheduleHints.parse(sched_hints, from_user)
        if status != 200:
          return status, None, err
        data['user_schedule_hints'] = sched_hints
    else:
      user_sched_hints = data.get('user_schedule_hints')
      sys_sched_hints = data.get('sys_schedule_hints')
      if user_sched_hints:
        _, data['user_schedule_hints'], _ = ContainerScheduleHints.parse(user_sched_hints, from_user)
      if sys_sched_hints:
        _, data['sys_schedule_hints'], _ = ContainerScheduleHints.parse(sys_sched_hints, from_user)
    if data['type'] == ContainerType.SERVICE.value:
      from container.service import Service
      return 200, Service(**data), None
    if data['type'] == ContainerType.JOB.value:
      from container.job import Job
      try:
        return 200, Job(**data), None
      except ValueError as e:
        return 400, None, str(e)
    return 400, 'Unknown container type: %s'%data['type'], None

  def __init__(self, id, appliance, type, image, resources, cmd=None, args=[], env={},
               volumes=[], network_mode=NetworkMode.HOST, endpoints=[], ports=[],
               state=ContainerState.SUBMITTED, is_privileged=False, force_pull_image=True,
               dependencies=[], last_update=None, user_schedule_hints=None, sys_schedule_hints=None,
               deployment=None, *aargs, **kwargs):
    self.__id = id
    self.__appliance = appliance
    self.__type = type if isinstance(type, ContainerType) else ContainerType(type)
    self.__image = image
    self.__resources = Resources(**resources)
    self.__cmd = cmd and str(cmd)
    self.__args = [a and str(a) for a in args]
    if self.__cmd and self.__args:
      raise ValueError("Cannot specify both 'cmd' and 'args'")
    self.__env = {k: v if v and isinstance(v, str) else json.dumps(v) for k, v in env.items()}
    self.__volumes = [ContainerVolume(**v) for v in volumes]
    self.__network_mode = network_mode if isinstance(network_mode, NetworkMode) \
                          else NetworkMode(network_mode.upper())
    self.__endpoints = [Endpoint(**e) for e in endpoints]
    self.__ports = [Port(**p) for p in ports]
    self.__state = state if isinstance(state, ContainerState) else ContainerState(state)
    self.__is_privileged = is_privileged
    self.__force_pull_image = force_pull_image
    self.__dependencies = list(dependencies)

    if isinstance(user_schedule_hints, dict):
      self.__user_schedule_hints = ContainerScheduleHints(**user_schedule_hints)
    elif isinstance(user_schedule_hints, ContainerScheduleHints):
      self.__user_schedule_hints = user_schedule_hints
    else:
      self.__user_schedule_hints = ContainerScheduleHints()

    if isinstance(sys_schedule_hints, dict):
      self.__sys_schedule_hints = ContainerScheduleHints(**sys_schedule_hints)
    elif isinstance(sys_schedule_hints, ContainerScheduleHints):
      self.__sys_schedule_hints = sys_schedule_hints
    else:
      self.__sys_schedule_hints = ContainerScheduleHints()

    if isinstance(deployment, dict):
      self.__deployment = Deployment(**deployment)
    elif isinstance(deployment, Deployment):
      self.__deployment = deployment
    else:
      self.__deployment = Deployment()

    self.__last_update = parse_datetime(last_update)

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
  def user_schedule_hints(self):
    """
    User-specified container scheduling hints
    ---
    type: ContainerScheduleHints

    """
    return self.__user_schedule_hints

  @property
  @swagger.property
  def sys_schedule_hints(self):
    """
    System container scheduling hints
    ---
    type: ContainerScheduleHints

    """
    return self.__sys_schedule_hints

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

  @property
  def last_update(self):
    return self.__last_update

  @property
  def host_volumes(self):
    """
    Host volumes
    ---
    type: list
    items: ContainerVolume

    """
    return [v for v in self.volumes if v.type == ContainerVolumeType.HOST]

  @property
  def persistent_volumes(self):
    """
    Persistent volumes
    ---
    type: list
    items: ContainerVolume

    """
    return [v for v in self.volumes if v.type == ContainerVolumeType.PERSISTENT]

  @image.setter
  def image(self, image):
    assert isinstance(image, str)
    self.__image = image

  @resources.setter
  def resources(self, resources):
    self.__resources = resources

  @appliance.setter
  def appliance(self, app):
    assert isinstance(app, str) or isinstance(app, appliance.Appliance)
    self.__appliance = app

  @endpoints.setter
  def endpoints(self, endpoints):
    self.__endpoints = list(endpoints)

  @state.setter
  def state(self, state):
    self.__state = state if isinstance(state, ContainerState) else ContainerState(state)

  @sys_schedule_hints.setter
  def sys_schedule_hints(self, hints):
    assert isinstance(hints, ContainerScheduleHints)
    self.__sys_schedule_hints = hints

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
    return dict(id=self.id,
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id,
                type=self.type.value,
                image=self.image, resources=self.resources.to_render(),
                endpoints=[e.to_render() for e in self.endpoints],
                state=self.state.value,
                dependencies=self.dependencies,
                user_schedule_hints=self.user_schedule_hints.to_render(),
                sys_schedule_hints=self.sys_schedule_hints.to_render(),
                deployment=self.deployment.to_render())

  def to_save(self):
    return dict(id=self.id,
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id,
                type=self.type.value,
                image=self.image, resources=self.resources.to_save(),
                cmd=self.cmd, args=self.args, env=self.env,
                volumes=[v.to_save() for v in self.volumes],
                network_mode=self.network_mode.value,
                endpoints=[e.to_save() for e in self.endpoints],
                ports=[p.to_save() for p in self.ports],
                state=self.state.value, is_privileged=self.is_privileged,
                force_pull_image=self.force_pull_image, dependencies=self.dependencies,
                last_update=self.last_update and self.last_update.isoformat(),
                user_schedule_hints=self.user_schedule_hints.to_save(),
                sys_schedule_hints=self.sys_schedule_hints.to_save(),
                deployment=self.deployment.to_save())

  def __hash__(self):
    return hash((self.id, self.appliance))

  def __eq__(self, other):
    return isinstance(other, Container) \
            and self.id == other.id \
            and ((isinstance(self.appliance, appliance.Appliance)
                 and isinstance(other.appliance, appliance.Appliance)
                 and self.appliance == other.appliance)
              or (isinstance(self.appliance, str)
                 and isinstance(other.appliance, str)
                 and self.appliance == other.appliance))


@swagger.model
class Deployment:

  def __init__(self, ip_addresses=[], placement=None):
    self.__ip_addresses = list(ip_addresses)
    if isinstance(placement, dict):
      self.__placement = Placement(**placement)
    elif isinstance(placement, Placement):
      self.__placement = placement
    else:
      self.__placement = Placement()

  @property
  @swagger.property
  def placement(self):
    """
    Physical placement of the container
    ---
    type: Placement
    read_only: true

    """
    return self.__placement

  @property
  def ip_addresses(self):
    return list(self.__ip_addresses)

  def add_ip_address(self, ip_addr):
    self.__ip_addresses.append(ip_addr)

  def to_render(self):
    return dict(placement=self.placement.to_render())

  def to_save(self):
    return dict(ip_addresses=self.ip_addresses, placement=self.placement.to_render())


def get_short_ids(p):
  return re.compile(r'[^\\]+@(%s)'%Container.ID_PATTERN).findall(p) if p else []


def parse_container_short_id(p, appliance):
  if p and p[0] == '@': # add a sentinel
    p = ' ' + p
  return re.sub(r'([^@\\]+)@(%s)'%Container.ID_PATTERN,
                r'\1\2-%s.marathon.containerip.dcos.thisdcos.directory'%appliance,
                str(p)).strip().replace('\\@', '@')
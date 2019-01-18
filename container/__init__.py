import re
import json
import swagger

import appliance as app
import volume
import schedule

from enum import Enum


@swagger.enum
class ContainerType(Enum):
  """
  "service" for long-running containers, "job" for one-off containers

  """

  SERVICE = 'service'
  JOB = 'job'


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

  def __init__(self, src, dest, type=ContainerVolumeType.PERSISTENT, scope=volume.VolumeScope.LOCAL,
               *args, **kwargs):
    self.__src = src
    self.__dest = dest
    self.__type = type if isinstance(type, ContainerVolumeType) else ContainerVolumeType(type.upper())
    self.__scope = scope if isinstance(scope, volume.VolumeScope) else volume.VolumeScope(scope.upper())

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

  @property
  def scope(self):
    return self.__scope

  @scope.setter
  def scope(self, scope):
    assert isinstance(scope, volume.VolumeScope)
    self.__scope = scope

  def to_render(self):
    return dict(src=self.src, dest=self.dest, type=self.type.value)

  def to_save(self):
    return dict(**self.to_render(), scope=self.__scope.value)


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

  def __hash__(self):
    return hash((self.host, self.host_port, self.container_port, self.protocol, self.name))

  def __eq__(self, other):
    return isinstance(other, Endpoint) \
           and self.host == other.host \
           and self.host_port == other.host_port \
           and self.container_port == other.container_port \
           and self.name == other.name

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
    sched_hints = data.get('schedule_hints')
    if sched_hints:
      _, data['schedule_hints'], _ = ContainerScheduleHints.parse(sched_hints, from_user)
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

  def __init__(self, id, appliance, type, image, resources, instances=1, cmd=None, args=[], env={},
               volumes=[], network_mode=NetworkMode.HOST, endpoints=[], ports=[],
               is_privileged=False, force_pull_image=True, dependencies=[], schedule_hints=None,
               tasks=[], *aargs, **kwargs):
    self.__id = id
    assert isinstance(appliance, str) or isinstance(appliance, app.Appliance)
    self.__appliance = appliance
    self.__type = type if isinstance(type, ContainerType) else ContainerType(type)
    self.__image = image
    self.__resources = Resources(**resources)
    self.__instances = instances
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
    self.__is_privileged = is_privileged
    self.__force_pull_image = force_pull_image
    self.__dependencies = list(dependencies)

    if isinstance(schedule_hints, dict):
      self.__schedule_hints = ContainerScheduleHints(**schedule_hints)
    elif isinstance(schedule_hints, ContainerScheduleHints):
      self.__schedule_hints = schedule_hints
    else:
      self.__schedule_hints = ContainerScheduleHints()
    if all([isinstance(t, schedule.task.Task) for t in tasks]):
      self.__tasks = list(tasks)
    elif all([isinstance(t, dict) for t in tasks]):
      self.__tasks = [schedule.task.Task(container=self, **t) for t in tasks]

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
  def instances(self):
    """
    Number of instances created for the container
    ---
    type: int
    required: true
    default: 1
    example: 1

    """
    return self.__instances

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
  def schedule_hints(self):
    """
    container scheduling hints
    ---
    type: ContainerScheduleHints

    """
    return self.__schedule_hints

  @property
  @swagger.property
  def tasks(self):
    return list(self.__tasks)

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
  def appliance(self, appliance):
    assert isinstance(appliance, str) or isinstance(appliance, app.Appliance)
    self.__appliance = appliance

  @endpoints.setter
  def endpoints(self, endpoints):
    self.__endpoints = list(endpoints)

  @schedule_hints.setter
  def schedule_hints(self, hints):
    assert isinstance(hints, ContainerScheduleHints)
    self.__schedule_hints = hints

  def add_env(self, **env):
    self.__env.update(env)

  def add_tasks(self, *tasks):
    assert all([isinstance(t, schedule.task.Task) for t in tasks])
    self.__tasks = list(set(self.__tasks + list(tasks)))

  def to_render(self):
    return dict(id=self.id,
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id,
                type=self.type.value,
                image=self.image,
                resources=self.resources.to_render(),
                instances=self.instances,
                endpoints=[e.to_render() for e in self.endpoints],
                dependencies=self.dependencies,
                schedule_hints=self.schedule_hints.to_render(),
                tasks=[t.to_render() for t in self.tasks])

  def to_save(self):
    return dict(id=self.id,
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id,
                type=self.type.value,
                image=self.image,
                resources=self.resources.to_save(),
                instances=self.instances,
                cmd=self.cmd, args=self.args, env=self.env,
                volumes=[v.to_save() for v in self.volumes],
                network_mode=self.network_mode.value,
                endpoints=[e.to_save() for e in self.endpoints],
                ports=[p.to_save() for p in self.ports],
                is_privileged=self.is_privileged,
                force_pull_image=self.force_pull_image,
                dependencies=self.dependencies,
                schedule_hints=self.schedule_hints.to_save(),
                tasks=[t.to_save() for t in self.tasks])

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


def get_short_ids(p):
  return re.compile(r'[^\\]+@(%s)'%Container.ID_PATTERN).findall(p) if p else []


def parse_container_short_id(p, appliance):
  if p and p[0] == '@': # add a sentinel
    p = ' ' + p
  return re.sub(r'([^@\\]+)@(%s)'%Container.ID_PATTERN,
                r'\1\2-%s.marathon.containerip.dcos.thisdcos.directory'%appliance,
                str(p)).strip().replace('\\@', '@')
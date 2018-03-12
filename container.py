import uuid
import json

from enum import Enum
from tornado.httpclient import AsyncHTTPClient, HTTPError

from marathon import MarathonRequest
from chronos import ChronosRequest
from config import config, cluster_to_ip
from util import Singleton, MotorClient, Loggable
from util import message, error
from util import HTTP_METHOD_POST, HTTP_METHOD_DELETE
from util import APPLICATION_JSON


class ContainerState(Enum):

  UNKNOWN = 'unknown'
  PENDING = 'pending'
  SUBMITTED = 'submitted'
  WAITING = 'waiting'
  STAGING = 'staging'
  RUNNING = 'running'
  FAILED = 'failed'

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
    return dict(container_path=self.__container_path,
                host_path=self.__host_path,
                mode=self.__mode)

  def to_save(self):
    return self.to_render()


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


class PortMapping:

  def __init__(self, container_port, host_port=0, protocol='tcp'):
    self.__container_port = container_port
    self.__host_port = host_port
    self.__protocol = protocol

  @property
  def container_port(self):
    return self.__container_port

  @property
  def host_port(self):
    return self.__host_port

  @property
  def protocol(self):
    return self.__protocol

  def to_render(self):
    return dict(container_port=self.__container_port,
                host_port=self.__host_port, protocol=self.__protocol)

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

  def __init__(self, cpus, mem, disk=0):
    self.__cpus = cpus
    self.__mem = mem
    self.__disk = disk

  @property
  def cpus(self):
    return self.__cpus

  @property
  def mem(self):
    return self.__mem

  @property
  def disk(self):
    return self.__disk

  def to_render(self):
    return dict(cpus=self.cpus, mem=self.mem, disk=self.disk)

  def to_save(self):
    return self.to_render()


class Container:

  MAX_N_WAITING = 10

  def __init__(self, id, image, resources, appliance=None, type='service',
               port_mappings=[], cmd=None, args=[], env={}, data=[],
               endpoints=[], volumes=[], state=ContainerState.UNKNOWN, cluster=None, host=None,
               is_privileged=False, force_pull_image=True, n_waiting=0, **kwargs):
    self.__id = id
    self.__appliance = appliance
    self.__type = type
    self.__image = image
    self.__resources = Resources(**resources)
    self.__port_mappings = [PortMapping(**p) for p in port_mappings]
    self.__cmd = cmd
    self.__args = list(args)
    self.__env = dict(env)
    self.__data = list(data)
    self.__endpoints = [Endpoint(**e) for e in endpoints]
    self.__volumes = [Volume(**v) for v in volumes]
    self.__state = state if isinstance(state, ContainerState) else ContainerState(state)
    self.__cluster = cluster
    self.__host = host
    self.__is_privileged = is_privileged
    self.__force_pull_image = force_pull_image
    self.__n_waiting = n_waiting

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
  def port_mappings(self):
    return list(self.__port_mappings)

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
  def data(self):
    return list(self.__data)

  @property
  def endpoints(self):
    return list(self.__endpoints)

  @property
  def volumes(self):
    return list(self.__volumes)

  @property
  def state(self):
    return self.__state

  @property
  def cluster(self):
    return self.__cluster

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
  def n_waiting(self):
    return self.__n_waiting

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

  @port_mappings.setter
  def port_mappings(self, port_mappings):
    self.__port_mappings = list(port_mappings)

  @endpoints.setter
  def endpoints(self, endpoints):
    self.__endpoints = list(endpoints)

  @state.setter
  def state(self, state):
    self.__state = state if isinstance(state, ContainerState) else ContainerState(state)

  @cluster.setter
  def cluster(self, cluster):
    self.__cluster = cluster

  @host.setter
  def host(self, host):
    self.__host = host

  def add_env(self, **env):
    self.__env.update(env)

  def increment_waiting_bit(self):
    self.__n_waiting += 1

  def reset_waiting_bit(self):
    self.__n_waiting = 0

  def to_render(self):
    return dict(id=self.id, type=self.type,
                image=self.image, resources=self.resources.to_render(),
                port_mappings=[p.to_render() for p in self.port_mappings],
                endpoints=[e.to_render() for e in self.endpoints],
                volumes=[v.to_render() for v in self.volumes],
                cmd=self.cmd, args=self.args, env=self.env,
                data=self.data, state=self.state.value,
                cluster=self.cluster, host=self.host,
                is_privileged=self.is_privileged, force_pull_image=self.force_pull_image)

  def to_save(self):
    return dict(id=self.id, appliance=self.appliance, type=self.type,
                image=self.image, resources=self.resources.to_save(),
                port_mappings=[p.to_save() for p in self.port_mappings],
                endpoints=[e.to_save() for e in self.endpoints],
                volumes=[v.to_save() for v in self.volumes],
                cmd=self.cmd, args=self.args, env=self.env,
                data=self.data, state=self.state.value,
                cluster=self.cluster, host=self.host,
                is_privileged=self.is_privileged,
                force_pull_image=self.force_pull_image,
                n_waiting=self.n_waiting)

  def __str__(self):
    return '%s--%s'%(self.appliance, self.id)


class ContainerManager(Loggable, metaclass=Singleton):

  def __init__(self):
    self.__contr_col = MotorClient().requester.container
    self.__http_cli = AsyncHTTPClient()

  async def get_container(self, app_id, contr_id):
    contr = await self.__contr_col.find_one(dict(id=contr_id, appliance=app_id))
    if not contr:
      return 404, error("Container '%s' is not found"%contr_id)
    return 200, Container(**contr)

  async def get_containers(self, app_id):
    contrs = [Container(**c) async for c in self.__contr_col.find(dict(appliance=app_id))]
    if not contrs:
      return 404, error("Containers of appliance '%s' are not found"%app_id)
    return 200, contrs

  async def delete_containers(self, app_id):
    await self.__contr_col.delete_many(dict(appliance=app_id))
    return 200, message("Containers of appliance '%s' have been deleted"%app_id)

  async def save_container(self, contr, upsert=True):
    assert isinstance(contr, Container)
    await self.__contr_col.replace_one(dict(id=contr.id, appliance=contr.appliance),
                                       contr.to_save(), upsert=upsert)

  async def submit_container(self, contr):
    assert isinstance(contr, Container)
    try:
      req = dict(requesterAddress='%s:%d/appliance/%s/container/%s/offers'%(
      config['host'], config['port'], contr.appliance, contr.id),
                 coordinatorAddress=config['meta_orchestrator'],
                 name='%s/%s'%(contr.appliance, contr.id),
                 resources='cpus:%.1f;mem:%.1f'%(contr.resources.cpus,
                                                 contr.resources.mem),
                 dockerImage=contr.image,
                 globalFrameworkId=uuid.uuid4().hex,
                 data=[] if contr.cluster else contr.data)
      await self.__http_cli.fetch(config['meta_orchestrator'],
                                  method=HTTP_METHOD_POST,
                                  headers=APPLICATION_JSON,
                                  body=json.dumps(req))
      return 200, message("Container '%s' has been submitted"%contr.id)
    except HTTPError as e:
      return (e.response.code, e.response.body) if e.response else (500, None)

  async def provision_container(self, contr, network):
    assert isinstance(contr, Container)
    assert contr.type in ('service', 'job')
    if contr.type == 'service':
      req = MarathonRequest(contr, network).to_render()
      uri = 'http://%s:9090/v2/apps'%cluster_to_ip[contr.cluster]
    elif contr.type == 'job':
      req = ChronosRequest(contr).to_render()
      uri = 'http://%s:8080/v1/scheduler/iso8601'%cluster_to_ip[contr.cluster]
      self.logger.debug('Chronos request:')
      self.logger.debug(json.dumps(req))
    headers = dict(method=HTTP_METHOD_POST,
                   headers=APPLICATION_JSON,
                   body=json.dumps(req))
    if contr.type == 'service':
      headers.update(dict(auth_username=config['username'],
                          auth_password=config['password']))
    try:
        r = await self.__http_cli.fetch(uri, **headers)
        return r.code, r.body
    except HTTPError as e:
        self.logger.warn(e.response.code, e.response.body)
        return e.response.code, e.response.body

  async def deprovision_container(self, contr):
    if not contr.cluster:
      return 200, None
    assert isinstance(contr, Container)
    host = cluster_to_ip[contr.cluster]
    if contr.type == 'service':
      url = 'http://%s:9090/v2/apps/%s'%(host, contr)
    elif contr.type == 'job':
      url = 'http://%s:8080/v1/scheduler/job/%s'%(host, contr)
    try:
      headers = dict(method=HTTP_METHOD_DELETE)
      if contr.type == 'service':
        headers.update(dict(auth_username=config['username'],
                            auth_password=config['password']))
      r = await self.__http_cli.fetch(url, ** headers)
      return r.code, r.body
    except HTTPError as e:
      return e.response.code, e.response.body



import json
import swagger

from util import parse_container_short_id
from container.base import Container, ContainerState, Endpoint, NetworkMode


@swagger.model
class HealthCheck:
  """
  Heath check performed on PIVOT services after they are up and running

  """

  def __init__(self, path='/', protocol='MESOS_TCP', port_index=0, max_consecutive_failures=3,
               grace_period_seconds=300, interval_seconds=60, timeout_seconds=20):
    self.__path = path
    self.__protocol = protocol
    self.__port_index = port_index
    self.__max_consecutive_failures = max_consecutive_failures
    self.__grace_period_seconds = grace_period_seconds
    self.__interval_seconds = interval_seconds
    self.__timeout_seconds = timeout_seconds

  @property
  @swagger.property
  def path(self):
    """
    Path to the service endpoint
    ---
    type: str
    default: /
    example: /ping

    """
    return self.__path

  @property
  @swagger.property
  def protocol(self):
    """
    Protocol for health check. Available protocols are `HTTP`, `HTTPS`, 'TCP`,
    `MESOS_HTTP`, `MESOS_HTTPS`, `MESOS_TCP`
    ---
    type: str
    default: MESOS_TCP
    example: MESOS_TCP

    """
    return self.__protocol

  @property
  @swagger.property
  def port_index(self):
    """
    The index of the port in the service's array of `Port` definitions for health check
    ---
    type: int
    default: 0
    example: 0

    """
    return self.__port_index

  @property
  @swagger.property
  def max_consecutive_failures(self):
    """
    The maximum number of consecutive failures before the service marked as `unhealthy`
    ---
    type: int
    default: 3
    example: 3

    """
    return self.__max_consecutive_failures

  @property
  @swagger.property
  def grace_period_seconds(self):
    """
    The grace periods (in seconds) between when the service is terminated gracefully and
    when it is killed forcefully, when the service is marked as `unhealthy`
    ---
    type: int
    default: 300
    example: 300

    """
    return self.__grace_period_seconds

  @property
  @swagger.property
  def interval_seconds(self):
    """
    Interval (in seconds) between health checks
    ---
    type: int
    default: 60
    example: 60

    """
    return self.__interval_seconds

  @property
  @swagger.property
  def timeout_seconds(self):
    """
    Maximum number of seconds to wait for the response from the service, after which it
    will be marked as `unhealthy` regardless of the response
    ---
    type: int
    default: 20
    example: 20

    """
    return self.__timeout_seconds

  def to_render(self):
    return dict(path=self.path, protocol=self.protocol, port_index=self.port_index,
                max_consecutive_failures=self.max_consecutive_failures,
                grace_period_seconds=self.grace_period_seconds,
                interval_seconds=self.interval_seconds,
                timeout_seconds=self.timeout_seconds)

  def to_save(self):
    return self.to_render()

  def to_request(self):
    return dict(path=self.path, protocol=self.protocol, portIndex=self.port_index,
                maxConsecutiveFailures=self.max_consecutive_failures,
                gracePeriodSeconds=self.grace_period_seconds,
                intervalSeconds=self.interval_seconds,
                timeoutSeconds=self.timeout_seconds)


@swagger.model
class Service(Container):
  """
  PIVOT service

  """

  def __init__(self, instances=1, labels={}, health_checks=[], default_health_checks=True,
               minimum_capacity=1., **kwargs):
    super(Service, self).__init__(**kwargs)
    self.__instances = instances
    self.__labels = dict(labels)
    self.__health_checks = [HealthCheck(**hc) for hc in health_checks]
    self.__default_health_checks = default_health_checks
    self.__minimum_capacity = minimum_capacity

  @property
  @swagger.property
  def instances(self):
    """
    Number of instances created for the service
    ---
    type: int
    default: 1
    example: 1

    """
    return self.__instances

  @property
  @swagger.property
  def labels(self):
    """
    Key-value label(s) assigned to the service for facilitating service discovery
    ---
    type: dict
    default: {}
    example:
      region: us-east
    """
    return dict(self.__labels)

  @property
  @swagger.property
  def health_checks(self):
    """
    Health checks to be performed on the service
    ---
    type: list
    items: HealthCheck
    default: a list of HealthCheck instances with default values on every defined `Port`
    example: HealthCheck

    """
    return list(self.__health_checks)

  @property
  @swagger.property
  def default_health_checks(self):
    """
    Whether to perform default health checks on the service
    ---
    type: bool
    default: true
    example: true

    """
    return self.__default_health_checks

  @property
  @swagger.property
  def minimum_capacity(self):
    """
    The minimum percentage of service instances that must stay `healthy` in order to mark
    the service as `healthy`
    ---
    type: float
    minimum: 0.0
    maximum: 1.0
    default: 1.0
    example: 1.0

    """
    return self.__minimum_capacity

  @classmethod
  async def parse(cls, body, cluster_mgr):
    if isinstance(body, str):
      body = json.loads(body.decode('utf-8'))
    body = body['app']
    tasks, min_capacity = body['tasks'], body['upgradeStrategy']['minimumHealthCapacity']
    # parse state
    state = ContainerState.determine_state([t['state'] for t in tasks], min_capacity)
    if state == ContainerState.RUNNING and body.get('healthChecks', []):
      tasks_healthy, tasks_unhealthy = body['tasksHealthy'], body['tasksUnhealthy']
      instances = body['instances']
      if tasks_healthy/instances < min_capacity:
        if tasks_healthy + tasks_unhealthy < instances:
          state = ContainerState.PENDING
        else:
          state = ContainerState.FAILED
    # parse endpoints
    endpoints, rack, host = [], None, None
    if state == ContainerState.RUNNING:
      for t in tasks:
        hosts = await cluster_mgr.find_hosts_by_attributes(hostname=t['host'])
        if not hosts: continue
        host, rack = hosts[0], hosts[0].attributes.get('rack', None)
        public_ip = host.attributes.get('public_ip', None)
        if not public_ip: continue
        if 'portDefinitions' in body:
          for i, p in enumerate(body['portDefinitions']):
            endpoints += [Endpoint(public_ip, p['port'], t['ports'][i], p['protocol'])]
        else:
          for i, p in enumerate(body['container']['portMappings']):
            endpoints += [Endpoint(public_ip, p['containerPort'], t['ports'][i],
                                   p['protocol'])]

    _, appliance, id = body['id'].split('/')
    return dict(id=id, appliance=appliance, state=state,
                rack=rack, host=host and host.hostname, endpoints=endpoints)

  def add_health_check(self, hc):
    self.__health_checks.append(hc)

  def to_render(self):
    return dict(**super(Service, self).to_render(),
                instances=self.instances,
                labels=self.labels,
                health_checks=[hc.to_render() for hc in self.health_checks],
                default_health_checks=self.default_health_checks,
                minimum_capacity=self.minimum_capacity)

  def to_save(self):
    self._add_default_health_checks()
    return dict(**super(Service, self).to_save(),
                instances=self.instances,
                labels=self.labels,
                health_checks=[hc.to_save() for hc in self.health_checks],
                default_health_checks=self.default_health_checks,
                minimum_capacity=self.minimum_capacity)

  def to_request(self):
    self._add_default_health_checks()
    r = dict(id=str(self), instances=self.instances,
             **self.resources.to_request(),
             env={k: parse_container_short_id(v, self.appliance)
                  for k, v in self.env.items()},
             labels=self.labels,
             requirePorts=len(self.ports) > 0,
             acceptedResourceRoles=[ "slave_public", "*" ],
             container=dict(type='DOCKER',
                            volumes=[v.to_request() for v in self.volumes],
                            docker=dict(image=self.image,
                                        privileged=self.is_privileged,
                                        forcePullImage=self.force_pull_image)),
             healthChecks=[hc.to_request() for hc in self.health_checks],
             upgradeStrategy=dict(
               minimumHealthCapacity=self.minimum_capacity,
               maximumOverCapacity=1.))
    if self.cmd:
      r['cmd'] = ' '.join([parse_container_short_id(p, self.appliance)
                           for p in self.cmd.split()])
    if self.args:
      r['args'] = [parse_container_short_id(a, self.appliance)
                   for a in self.args if str(a).strip()]
    # set network mode
    if self.network_mode == NetworkMode.HOST:
      r['networks'] = [dict(mode='host')]
    elif self.network_mode == NetworkMode.BRIDGE:
      r['networks'] = [dict(mode='container/bridge')]
    elif self.network_mode == NetworkMode.CONTAINER:
      r['networks'] = [dict(mode='container', name='dcos')]
    # set port definitions
    if self.network_mode == NetworkMode.HOST:
      r['portDefinitions'] = [dict(protocol=p.protocol, port=p.container_port)
                              for i, p in enumerate(self.ports)]
    else:
      port_mappings = [dict(protocol=p.protocol, hostPort=p.host_port,
                            containerPort=p.container_port)
                       for i, p in enumerate(self.ports)]
      r['container']['docker']['portMappings'] = port_mappings
    if self.rack:
      r.setdefault('constraints', []).append(['rack', 'CLUSTER', self.rack])
    if self.host:
      r.setdefault('constraints', []).append(['hostname', 'CLUSTER', self.host])
    return r

  def _add_default_health_checks(self):
    if not self.health_checks and self.default_health_checks:
      for i, p in enumerate(self.ports):
        if p.protocol != 'tcp':
          continue
        self.add_health_check(HealthCheck(port_index=i))

  def __str__(self):
    return '/%s/%s'%(self.appliance, self.id)



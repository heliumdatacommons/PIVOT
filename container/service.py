import json

from util import parse_container_short_id
from container.base import Container, ContainerState, Endpoint, NetworkMode


class HealthCheck:

  def __init__(self, path, protocol='TCP', port_index=0, max_consecutive_failures=3,
               grace_period_seconds=300, interval_seconds=60, timeout_seconds=20):
    self.__path = path
    self.__protocol = protocol
    self.__port_index = port_index
    self.__max_consecutive_failures = max_consecutive_failures
    self.__grace_period_seconds = grace_period_seconds
    self.__interval_seconds = interval_seconds
    self.__timeout_seconds = timeout_seconds

  @property
  def path(self):
    return self.__path

  @property
  def protocol(self):
    return self.__protocol

  @property
  def port_index(self):
    return self.__port_index

  @property
  def max_consecutive_failures(self):
    return self.__max_consecutive_failures

  @property
  def grace_period_seconds(self):
    return self.__grace_period_seconds

  @property
  def interval_seconds(self):
    return self.__interval_seconds

  @property
  def timeout_seconds(self):
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


class Service(Container):

  def __init__(self, instances=1, labels={}, health_checks=[],
               maximum_capacity=1., minimum_capacity=1., **kwargs):
    super(Service, self).__init__(**kwargs)
    self.__instances = instances
    self.__labels = dict(labels)
    self.__health_checks = [HealthCheck(**hc) for hc in health_checks]
    self.__minimum_capacity = minimum_capacity
    self.__maximum_capacity = maximum_capacity

  @property
  def instances(self):
    return self.__instances

  @property
  def labels(self):
    return dict(self.__labels)

  @property
  def health_checks(self):
    return list(self.__health_checks)

  @property
  def maximum_capacity(self):
    return self.__maximum_capacity

  @property
  def minimum_capacity(self):
    return self.__minimum_capacity

  @classmethod
  def parse(cls, body, cluster):
    if isinstance(body, str):
      body = json.loads(body.decode('utf-8'))
    body = body['app']
    tasks, min_capacity = body['tasks'], body['upgradeStrategy']['minimumHealthCapacity']
    # parse state
    state = ContainerState.determine_state([t['state'] for t in tasks], min_capacity)
    if state == ContainerState.RUNNING and body.get('healthChecks', []):
      tasks_healthy, instances = body['tasksHealthy'], body['instances']
      if tasks_healthy/instances < min_capacity:
        state = ContainerState.FAILED
    # parse endpoints
    endpoints, racks, hosts = [], [], []
    if state == ContainerState.RUNNING:
      for t in tasks:
        host = cluster.find_host_by_attribute('hostname', t['host'])
        if not host: continue
        host, rack = host[0], host[0].attributes.get('rack', None)
        public_ip = host.attributes.get('public_ip', None)
        if rack and rack not in racks:
          racks += [rack]
        if public_ip not in hosts:
          hosts += [public_ip]
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
                racks=racks, hosts=hosts, endpoints=endpoints)

  def add_health_check(self, hc):
    self.__health_checks.append(hc)

  def to_render(self):
    return dict(**super(Service, self).to_render(),
                instances=self.instances,
                labels=self.labels,
                health_checks=[hc.to_render() for hc in self.health_checks],
                maximum_capacity=self.__maximum_capacity,
                minimum_capacity=self.minimum_capacity)

  def to_save(self):
    return dict(**super(Service, self).to_save(),
                instances=self.instances,
                labels=self.labels,
                health_checks=[hc.to_save() for hc in self.health_checks],
                maximum_capacity=self.__maximum_capacity,
                minimum_capacity=self.minimum_capacity)

  def to_request(self):
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
               maximumOverCapacity=self.maximum_capacity))
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
      r['portDefinitions'] = [dict(protocol=p.protocol, port=p.container_port,
                                   labels={"VIP_%d"%i: "/:%d"%p.load_balanced_port})
                              for i, p in enumerate(self.ports)]
    else:
      port_mappings = [dict(protocol=p.protocol, hostPort=p.host_port,
                            containerPort=p.container_port,
                            labels={"VIP_%d"%i: "/:%d"%p.load_balanced_port})
                       for i, p in enumerate(self.ports)]
      r['container']['docker']['portMappings'] = port_mappings
    if self.rack:
      r.setdefault('constraints', []).append(['rack', 'CLUSTER', self.rack])
    if self.host:
      r.setdefault('constraints', []).append(['hostname', 'CLUSTER', self.host])
    return r

  def __str__(self):
    return '/%s/%s'%(self.appliance, self.id)



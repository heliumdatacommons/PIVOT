import datetime as dt

from appliance import Appliance
from container import NetworkMode, Endpoint
from container.service import Service
from container.job import Job
from volume import VolumeScope
from cluster.manager import ClusterManager
from schedule.task import Task, ServiceTask, JobTask, TaskState
from commons import APIManager
from config import config


class GeneralTaskManager(APIManager):

  def __init__(self):
    super(GeneralTaskManager, self).__init__()
    self.__cluster_mgr = ClusterManager()

  async def update_task(self, task):
    assert isinstance(task, Task)
    if not task.mesos_task_id:
      return 400, None, "Mesos task ID is not set"
    api, endpoint = config.mesos, '%s/tasks?task_id=%s'%(config.mesos.endpoint, task.mesos_task_id)
    cluster_mgr = self.__cluster_mgr
    status, body, err = await self.http_cli.get(api.host, api.port, endpoint)
    if status != 200:
      return status, None, err

    async def parse_mesos_response(body):
      states = body['tasks']
      if len(states) == 0:
        self.logger.debug("Task '%s' is not found"%task.mesos_task_id)
        return
      state = states[0]
      task.state = TaskState(state['state'].upper())
      agent = await cluster_mgr.get_agent(state['slave_id'])
      if not agent:
        await cluster_mgr.update()
        agent = await cluster_mgr.get_agent(state['slave_id'])
      task.placement = agent.locality.clone()
      hostname = agent.fqdn or agent.public_ip
      if task.container.network_mode == NetworkMode.HOST:
        endpoints = [Endpoint(hostname, p['number'], p['number'], task.id, p['protocol'])
                     for p in state.get('discovery', {}).get('ports', {}).get('ports', [])]
      else:
        endpoints = [Endpoint(hostname, p['container_port'], p['host_port'], task.id, p['protocol'])
                     for p in state.get('container', {}).get('docker', {}).get('port_mappings', [])]
      task.update_endpoints(*endpoints)

    await parse_mesos_response(body)
    return 200, task, None


class ServiceTaskManager(APIManager):
  
  def __init__(self):
    super(ServiceTaskManager, self).__init__()
    self.__cluster_mgr = ClusterManager()

  async def update_service_task(self, task):
    assert isinstance(task, ServiceTask)
    api, app = config.marathon, task.container.appliance
    assert isinstance(app, Appliance)
    endpoint = '%s/apps/%s/%s'%(api.endpoint, app.id, task.id)
    cluster_mgr = self.__cluster_mgr

    async def parse_marathon_response(body):
      states = body['app']['tasks']
      if len(states) == 0:
        self.logger.debug("Task '%s' is not found" % task.mesos_task_id)
        return
      state = states[-1]
      task.mesos_task_id, task.state = state['id'], TaskState(state['state'].upper())
      agent = await cluster_mgr.get_agent(state['slaveId'])
      if not agent:
        await cluster_mgr.update()
        agent = await cluster_mgr.get_agent(state['slaveId'])
      task.placement = agent.locality.clone()
      hostname = agent.fqdn or agent.public_ip
      if task.container.network_mode == NetworkMode.HOST:
        endpoints = [Endpoint(hostname, p, p) for p in state.get('ports', [])]
      else:
        endpoints = [Endpoint(hostname, p['containerPort'], p['servicePort'], p['protocol'])
                     for p in body['app']['container'].get('portMappings', [])]
      task.update_endpoints(*endpoints)

    status, body, err = await self.http_cli.get(api.host, api.port, endpoint)
    if status == 404:
      return 404, task, "Task '%s' is not found"%task
    if status != 200:
      self.logger.debug(err)
      return status, task, err
    await parse_marathon_response(body)
    return 200, task, None

  async def launch_service_task(self, task):
    assert isinstance(task, ServiceTask)
    app_id = task.container.appliance.id
    api, endpoint = config.marathon, '%s/apps/%s/%s'%(config.marathon.endpoint, app_id, task.id)
    resp = await self.http_cli.put(api.host, api.port, endpoint, self._create_request(task))
    task.launch_time, task.state = dt.datetime.now(), TaskState.TASK_SUBMITTED
    return resp

  async def delete_service_task(self, task):
    assert isinstance(task, Task)
    api, app = config.marathon, task.container.appliance
    assert isinstance(app, Appliance)
    endpoint = '%s/apps/%s/%s?force=true'%(api.endpoint, app.id, task.id)
    return await self.http_cli.delete(api.host, api.port, endpoint)

  def _create_request(self, task):
    assert isinstance(task, ServiceTask)
    app, contr = task.container.appliance, task.container
    assert isinstance(app, Appliance)
    assert isinstance(contr, Service)
    params = [dict(key='hostname', value=str(task.id)),
              dict(key='rm', value='true'),
              dict(key='oom-kill-disable', value='true')]
    # config persistent volumes
    data_p, p_vols = app.data_persistence, contr.persistent_volumes
    if data_p and len(p_vols) > 0:
      params += dict(key='volume-driver', value=data_p.volume_type.driver),
      params += [dict(key='volume',
                      value=('%s-'%app.id if v.scope == VolumeScope.LOCAL else '')
                            + '%s:%s'%(v.src, v.dest)) for v in p_vols]
    req = dict(id='/%s/%s'%(app.id, task.id),
               env={str(k): str(v) for k, v in contr.env.items()},
               requirePorts=len(contr.ports) > 0,
               healthChecks=[contr.health_check.to_request] if contr.health_check else [],
               container=dict(type='DOCKER',
                              volumes=[dict(hostPath=v.src, containerPath=v.dest, mode='RW')
                                       for v in contr.host_volumes],
                              docker=dict(image=contr.image,
                                          privileged=contr.is_privileged,
                                          forcePullImage=contr.force_pull_image,
                                          parameters=params)),
               **contr.resources.to_request())
    if contr.args:
      req['args'] = contr.args
    elif contr.cmd:
      req['cmd'] = contr.cmd
    # config network mode
    network_mode = contr.network_mode
    if network_mode == NetworkMode.HOST:
      req['networks'] = [dict(mode='host')]
    elif network_mode == NetworkMode.BRIDGE:
      req['networks'] = [dict(mode='container/bridge')]
    elif network_mode == NetworkMode.CONTAINER:
      req['networks'] = [dict(mode='container', name='dcos')]
    # config ports
    if network_mode == NetworkMode.HOST:
      req['portDefinitions'] = [dict(protocol=p.protocol, port=p.container_port)
                                for p in contr.ports]
    else:
      req['container']['docker']['portMappings'] = [dict(protocol=p.protocol,
                                                         hostPort=p.host_port,
                                                         containerPort=p.container_port)
                                                    for i, p in enumerate(contr.ports)]
    # config placement
    hints = task.schedule_hints
    preemptible, placement = hints.preemptible, hints.placement
    constraints = req.setdefault('constraints', [])
    constraints += ['preemptible', 'CLUSTER', str(preemptible).lower()],
    if placement.host:
      constraints += ['hostname', 'CLUSTER', str(placement.host)],
    elif placement.zone:
      constraints += ['zone', 'CLUSTER', str(placement.zone)],
    elif placement.region:
      constraints += ['region', 'CLUSTER', str(placement.region)],
    elif placement.cloud:
      constraints += ['cloud', 'CLUSTER', str(placement.cloud)],
    return req


class JobTaskManager(APIManager):
  
  def __init__(self):
    super(JobTaskManager, self).__init__()
    self.__task_mgr = GeneralTaskManager()

  async def update_job_task(self, task):
    assert isinstance(task, JobTask)
    api, app = config.chronos, task.container.appliance
    assert isinstance(app, Appliance)
    endpoint = '%s/job/%s.%s'%(config.chronos.endpoint, app.id, task.id)
    task_mgr = self.__task_mgr
    status, resp, err = await self.http_cli.get(api.host, api.port, endpoint)
    if status != 200:
      return status, None, err
    if resp['taskId'] != 'null':
      task.mesos_task_id = resp['taskId']
      return await task_mgr.update_task(task)
    return 200, task, None

  async def launch_job_task(self, task):
    assert isinstance(task, JobTask)
    api, endpoint = config.chronos, '%s/iso8601'%config.chronos.endpoint
    resp = await self.http_cli.post(api.host, api.port, endpoint, self._create_request(task))
    task.launch_time, task.state = dt.datetime.now(), TaskState.TASK_SUBMITTED
    return resp

  async def kill_job_task(self, task):
    assert isinstance(task, Task)
    api, app = config.chronos, task.container.appliance
    assert isinstance(app, Appliance)
    endpoint = '%s/task/kill/%s.%s'%(api.endpoint, app.id, task.id)
    return await self.http_cli.delete(api.host, api.port, endpoint)

  async def delete_job_task(self, task):
    assert isinstance(task, Task)
    api, app = config.chronos, task.container.appliance
    assert isinstance(app, Appliance)
    endpoint = '%s/job/%s.%s'%(api.endpoint, app.id, task.id)
    return await self.http_cli.delete(api.host, api.port, endpoint)

  def _create_request(self, task):
    assert isinstance(task, JobTask)
    app, contr = task.container.appliance, task.container
    assert isinstance(app, Appliance)
    assert isinstance(contr, Job)
    params = [dict(key='hostname', value=str(task.id)),
              dict(key='rm', value='true'),
              dict(key='privileged', value=contr.is_privileged),]
    # config persistent volumes
    data_p, p_vols = app.data_persistence, contr.persistent_volumes
    if data_p and len(p_vols) > 0:
      params += dict(key='volume-driver', value=data_p.volume_type.driver),
      params += [dict(key='volume',
                      value=('%s-'%app.id if v.scope == VolumeScope.LOCAL else '')
                            + '%s:%s'%(v.src, v.dest)) for v in p_vols]
    # config ports
    params += [dict(key='publish', value='%d:%d/%s'%(p.host_port, p.container_port, p.protocol))
               for p in contr.ports]
    req = dict(name='%s.%s'%(app.id, task.id),
               schedule='R%d/%s/P%s'%(contr.repeats, contr.start_time, contr.interval),
               shell=bool(contr.cmd),
               retries=contr.retries,
               environmentVariables=[dict(name=k, value=v) for k, v in contr.env.items()],
               container=dict(type='DOCKER',
                              image=contr.image,
                              parameters=params,
                              volumes=[dict(hostPath=v.src, containerPath=v.dest, mode='RW')
                                       for v in contr.host_volumes],
                              forcePullImage=contr.force_pull_image),
               **contr.resources.to_request())
    if len(task.env) > 0:
      req['environmentVariables'] += [dict(name=k, value=v) for k, v in task.env.items()]
    if contr.args:
      req['arguments'] = contr.args
    elif contr.cmd:
      req['command'] = contr.cmd
    # config network
    network_mode = contr.network_mode
    if network_mode == NetworkMode.HOST or network_mode == NetworkMode.BRIDGE:
      req['container']['network'] = network_mode.value
    else:
      req['container']['network'] = 'USER'
      req['container']['networkName'] = 'dcos'
    # config placement
    hints = task.schedule_hints
    preemptible, placement = hints.preemptible, hints.placement
    constraints = req.setdefault('constraints', [])
    constraints += ['preemptible', 'EQUALS', str(preemptible).lower()],
    if placement.host:
      constraints += ['hostname', 'EQUALS', str(placement.host)],
    elif placement.zone:
      constraints += ['zone', 'EQUALS', str(placement.zone)],
    elif placement.region:
      constraints += ['region', 'EQUALS', str(placement.region)],
    elif placement.cloud:
      constraints += ['cloud', 'EQUALS', str(placement.cloud)],
    return req
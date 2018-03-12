import json

from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.ioloop import PeriodicCallback
from collections import deque

from container import Container, Endpoint, ContainerState, ContainerManager
from util import message, error
from util import Singleton, MotorClient, Loggable
from util import HTTP_METHOD_GET
from config import config, cluster_to_ip, ip_to_cluster


class Appliance:

  def __init__(self, id, containers=[], pending=[], **kwargs):
    self.__id = id
    self.__containers = list(containers)
    self.__pending = deque(pending)

  @property
  def id(self):
      return self.__id

  @property
  def containers(self):
    return self.__containers

  @property
  def pending(self):
    return list(self.__pending)

  @containers.setter
  def containers(self, contrs):
    self.__containers = list(contrs)

  def enqueue_pending_containers(self, contr_id):
    self.__pending.append(contr_id)

  def dequeue_pending_container(self):
    return self.__pending.popleft()

  def to_render(self):
    return dict(id=self.id,
                containers=[c if isinstance(c, str) else c.to_render()
                            for c in self.containers])

  def to_save(self):
    return dict(id=self.id,
                containers=[c if isinstance(c, str) else c.id for c in self.containers],
                pending=self.pending)


class ApplianceManager(Loggable, metaclass=Singleton):

  def __init__(self):
    self.__app_col = MotorClient().requester.appliance
    self.__http_cli = AsyncHTTPClient()
    self.__contr_mgr = ContainerManager()

  async def get_appliance(self, app_id, verbose=True):
    app = await self.__app_col.find_one(dict(id=app_id))
    if not app:
      return 404, error("Appliance '%s' is not found"%app_id)
    app = Appliance(**app)
    if verbose:
      status, contrs = await self.__contr_mgr.get_containers(app_id)
      if status == 200:
        app.containers = contrs
    return 200, app

  async def create_appliance(self, app):
    app = Appliance(**app)
    app_count = await self.__app_col.count(dict(id=app.id))
    if app_count > 0:
      return 409, error("Appliance '%s' already exists" % app.id)
    if not app.containers:
      return 400, error('No container in the Appliance "%s"'%app.id)

    app.containers = [Container(**c) for c in app.containers]
    for c in app.containers:
      c.appliance, c.state = app.id, ContainerState.PENDING
      app.enqueue_pending_containers(c.id)
      c.add_env(SCIDAS_DATA=','.join(c.data),
                SCIDAS_RESC_CPUS=str(c.resources.cpus),
                SCIDAS_RESC_MEM=str(c.resources.mem),
                SCIDAS_RESC_DISK=str(c.resources.disk))

    for c in app.containers:
      await self.__contr_mgr.save_container(c)
    await self.save_appliance(app)
    return 201, app

  async def delete_appliance(self, app_id):
    status, app = await self.get_appliance(app_id)
    if status != 200:
      return status, None, app
    status, resp = await self.__contr_mgr.delete_containers(app_id)
    self.__app_col.delete_one(dict(id=app_id))
    return 200, app, message("Appliance '%s' has been deleted"%app_id)

  async def accept_offer(self, app_id, contr_id, offers):
    status, resp = await self.get_appliance(app_id, verbose=False)
    if status != 200:
      return status, resp, resp
    app = resp
    status, resp = await self.__contr_mgr.get_container(app_id, contr_id)
    if status != 200:
      return status, resp, resp
    contr = resp
    offers = list(filter(lambda o: o['cpus'] and o['mem'] and o['disk']
                                   and o['cpus'] >= contr.resources.cpus
                                   and o['mem'] >= contr.resources.mem
                                   and o['disk'] >= contr.resources.disk,
                         offers))
    for o in offers:
      o['master'] = o['master'].split(':')[0]
    if contr.cluster:
      offers = list(filter(lambda o: o['master'] == cluster_to_ip[contr.cluster], offers))
    if not offers:
      app.enqueue_pending_containers(contr.id)
      return 200, app, contr
    offer = offers[0]
    contr.cluster = ip_to_cluster.get(offer['master'], None)
    contr.host = offer['agent']
    await self.__contr_mgr.save_container(contr)
    return 200, app, contr

  async def process_next_pending_container(self, app):
    if not app.pending:
      return
    contr_id = app.dequeue_pending_container()
    await self.save_appliance(app, upsert=False)
    _, contr = await self.__contr_mgr.get_container(app.id, contr_id)
    contr.state = ContainerState.SUBMITTED
    await self.__contr_mgr.save_container(contr)
    status, _ = await self.__contr_mgr.submit_container(contr)
    if status != 200:
      app.enqueue_pending_containers(contr.id)

  async def save_appliance(self, app, upsert=True):
    await self.__app_col.replace_one(dict(id=app.id), app.to_save(), upsert=upsert)


class ApplianceMonitor(Loggable):

  def __init__(self, interval=5000):
    self.__cb = PeriodicCallback(self._monitor_appliances, interval)
    self.__contr_col = MotorClient().requester.container
    self.__contr_mgr = ContainerManager()
    self.__app_mgr = ApplianceManager()
    self.__http_cli = AsyncHTTPClient()
    self.__job_summary = {}

  def start(self):
    self.__cb.start()

  def stop(self):
    self.__cb.stop()

  async def _monitor_appliances(self):

    async def get_chronos_job_summary(host):
      try:
        url = 'http://%s:8080/v1/scheduler/jobs/summary'%host
        r = await self.__http_cli.fetch(url, method=HTTP_METHOD_GET)
        return {j['name']: j['status'] if j['status'] in ('success', 'failure')
                                       else j['state'].lower().strip('1 ')
                for j in json.loads(r.body.decode('utf-8'))['jobs']}
      except HTTPError as e:
        return {}

    def parse_marathon_container_info(info):
      state, endpoints = ContainerState.UNKNOWN, []
      app = info['app']
      contr_ports = app['container']['portMappings']
      if app['tasks']:
        task = app['tasks'][0]
        host = task.get('host', None)
        for i, p in enumerate(contr_ports):
          endpoints += [Endpoint(host, p['containerPort'],
                                 task['ports'][i], p['protocol'])]
        if task['state']:
          task_state = task['state'].lower().replace('task_', '')
          if ContainerState.has_value(task_state):
            state = ContainerState(task_state)
          else:
            self.logger.warn(task_state)
      else:
        state = ContainerState.WAITING
      return state, endpoints

    async def handle_marathon_container(contr):
      if not contr.cluster:
        return
      url = 'http://%s:9090/v2/apps/%s'%(cluster_to_ip[contr.cluster], contr)
      state = ContainerState.PENDING
      try:
        r = await self.__http_cli.fetch(url,
                                        auth_username=config['username'],
                                        auth_password=config['password'],
                                        method=HTTP_METHOD_GET)
        info = json.loads(r.body.decode('utf-8'))
        state, contr.endpoints = parse_marathon_container_info(info)
        if state == ContainerState.WAITING:
          contr.increment_waiting_bit()
          if contr.n_waiting > Container.MAX_N_WAITING:
            # the container is waiting for deployment, likely that the assigned
            # host is out of resources
            await self.__contr_mgr.deprovision_container(contr)
            contr.state, contr.host = ContainerState.PENDING, None
            _, app = await self.__app_mgr.get_appliance(contr.appliance)
            app.enqueue_pending_containers(contr.id)
            await self.__app_mgr.save_appliance(app, upsert=False)
            await self.__app_mgr.process_next_pending_container(app)
            contr.reset_waiting_bit()
          else:
            await self.__contr_mgr.save_container(contr, upsert=False)
        else:
          contr.state = state
          contr.reset_waiting_bit()
          await self.__contr_mgr.save_container(contr, upsert=False)
      except HTTPError as e:
        if e.response.code == 404 and state and state == ContainerState.SUBMITTED:
          _, app = await self.__app_mgr.get_appliance(contr.appliance)
          if isinstance(app, str):
            await self.__contr_mgr.delete_containers(contr.appliance)
          elif isinstance(app, Appliance):
            app.enqueue_pending_containers(contr.id)
            await self.__app_mgr.save_appliance(app, upsert=False)
            await self.__app_mgr.process_next_pending_container(app)
        # else:
        #   app.dequeue_pending_container()
        #   await self.__app_mgr.save_appliance(app, upsert=False)
        #   self.logger.warn(e.response.code, e.response.body)

    async def handle_chronos_container(contr):
      job_sum = await get_chronos_job_summary(cluster_to_ip[contr.cluster])
      self.__job_summary.update(job_sum)
      if str(contr) not in self.__job_summary:
        contr.state = ContainerState.SUBMITTED
      else:
        state = dict(running=ContainerState.RUNNING,
                     failure=ContainerState.FAILED,
                     queued=ContainerState.PENDING,
                     idle=ContainerState.WAITING).get(self.__job_summary[str(contr)],
                                                      ContainerState.UNKNOWN)
        self.logger.info('Container %s state: %s'%(contr, state))
        if contr.state == ContainerState.WAITING:
          contr.increment_waiting_bit()
          if contr.n_waiting > Container.MAX_N_WAITING:
            # the container is waiting for deployment, likely that the assigned
            # host is out of resources
            await self.__contr_mgr.deprovision_container(contr)
            contr.state, contr.host = ContainerState.PENDING, None
            _, app = await self.__app_mgr.get_appliance(contr.appliance)
            app.enqueue_pending_containers(contr.id)
            await self.__app_mgr.save_appliance(app, upsert=False)
            await self.__app_mgr.process_next_pending_container(app)
            contr.reset_waiting_bit()
          else:
            await self.__contr_mgr.save_container(contr, upsert=False)
        else:
          contr.state = state
          contr.reset_waiting_bit()
          await self.__contr_mgr.save_container(contr, upsert=False)

    async for c in self.__contr_col.find():
      contr = Container(**c)
      if contr.type == 'service':
        await handle_marathon_container(contr)
      elif contr.type == 'job':
        await handle_chronos_container(contr)


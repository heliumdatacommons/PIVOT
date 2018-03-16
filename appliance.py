import json

from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.ioloop import PeriodicCallback
from collections import deque

from container.base import Container, Endpoint, ContainerState
from container.manager import ContainerManager
from util import message, error
from util import Singleton, MotorClient, Loggable
from util import SecureAsyncHttpClient
from config import config, cluster_to_ip, ip_to_cluster


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
            await self.__contr_mgr._save_container_to_db(contr, upsert=False)
        else:
          contr.state = state
          contr.reset_waiting_bit()
          await self.__contr_mgr._save_container_to_db(contr, upsert=False)
      except HTTPError as e:
        if e.response.code == 404 and state and state == ContainerState.SUBMITTED:
          _, app = await self.__app_mgr.get_appliance(contr.appliance)
          if isinstance(app, str):
            await self.__contr_mgr._delete_containers_of_appliance_from_db(contr.appliance)
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
            await self.__contr_mgr._save_container_to_db(contr, upsert=False)
        else:
          contr.state = state
          contr.reset_waiting_bit()
          await self.__contr_mgr._save_container_to_db(contr, upsert=False)

    async for c in self.__contr_col.find():
      contr = Container(**c)
      if contr.type == 'service':
        await handle_marathon_container(contr)
      elif contr.type == 'job':
        await handle_chronos_container(contr)


import tornado
import functools

from tornado.ioloop import PeriodicCallback

from util import Singleton, Loggable, MotorClient, SecureAsyncHttpClient
from appliance.base import Appliance
from container.base import ContainerType, ContainerState
from container.manager import ContainerManager


class ApplianceManager(Loggable, metaclass=Singleton):

  def __init__(self, config):
    self.__config = config
    self.__http_cli = SecureAsyncHttpClient(config)
    self.__app_col = MotorClient().requester.appliance
    self.__contr_col = MotorClient().requester.container
    self.__contr_mgr = ContainerManager(config)
    self.__app_monitor = ApplianceMonitor(self, self.__contr_mgr)

  async def get_appliance(self, app_id):
    status, app, err = await self._get_appliance_from_db(app_id)
    if status != 200:
      return status, app, err
    app = Appliance(**app)
    _, app.containers, _ = await self.__contr_mgr.get_containers(app_id)
    return 200, app, None

  async def create_appliance(self, data):
    status, _, _ = await self._get_appliance_from_db(data['id'])
    if status == 200:
      return 409, None, "Appliance '%s' already exists"%data['id']
    status, app, err = self._instantiate_appliance(data)
    if err:
      self.logger.error(err)
      return status, None, err
    for c in app.containers:
      status, msg, err = await self.__contr_mgr.create_container(c.to_render())
      if err:
        self.logger.error(err)
        return status, None, err
    status, msg, err = await self.save_appliance(app, True)
    if err:
      self.logger.error(err)
      return status, None, err
    self.logger.info(msg)
    self.logger.info("Start monitoring appliance '%s'"%app)
    self.__app_monitor.spawn(app)
    return 201, app, None

  async def delete_appliance(self, app_id):
    status, app, err = await self._get_appliance_from_db(app_id)
    if err:
      self.logger.error(err)
      return status, None, err
    self.logger.info("Stop monitoring appliance '%s'"%app_id)
    self.__app_monitor.stop(app_id)
    status, msg, err = await self.__contr_mgr.delete_containers(appliance=app_id,
                                                                type=ContainerType.JOB.value)
    if err:
      self.logger.error(err)
      return 400, None, "Failed to deprovision jobs of appliance '%s'"%app_id
    self.logger.info(msg)
    status, msg, err = await self._deprovision_group(app_id)
    if err and status != 404:
      self.logger.error(err)
      return 400, None, "Failed to deprovision appliance '%s'"%app_id
    status, msg, err = await self._delete_appliance_from_db(app_id)
    if err:
      self.logger.error(err)
      return status, None, err
    self.logger.info(msg)
    return status, msg, None

  async def save_appliance(self, app, upsert=True):
    await self.__app_col.replace_one(dict(id=app.id), app.to_save(), upsert=upsert)
    return 200, "Appliance '%s' has been saved"%app, None

  async def restore_appliance(self, app_id):
    raise NotImplemented

  async def _get_appliance_from_db(self, app_id):
    app = await self.__app_col.find_one(dict(id=app_id))
    if not app:
      return 404, None, "Appliance '%s' is not found"%app_id
    return 200, app, None

  async def _delete_appliance_from_db(self, app_id):
    await self.__app_col.delete_one(dict(id=app_id))
    await self.__contr_col.delete_many(dict(appliance=app_id))
    return 200, "Appliance '%s' has been deleted"%app_id, None

  async def _deprovision_group(self, app_id):
    url = '%s/groups/%s?force=true'%(self.__config.url.service_scheduler, app_id)
    status, msg, err = await self.__http_cli.delete(url)
    if err:
      return status, None, err
    return 200, "Services of appliance '%s' have been deprovisioned"%app_id, None

  def _instantiate_appliance(self, data):
    app_id, containers = data['id'], data['containers']
    try:
      app = Appliance(app_id,
                      [self.__contr_mgr.instantiate_container(dict(**c, appliance=app_id))
                       for c in data['containers']])
      status, msg, err = app.dag.construct_graph(app.containers)
      if err:
        return status, None, err
      return 200, app, None
    except ValueError as e:
      return 422, None, str(e)


class ApplianceMonitor(Loggable):

  APP_MONITOR_INTERVAL = 5000

  def __init__(self, app_mgr, contr_mgr):
    self.__app_mgr = app_mgr
    self.__contr_mgr = contr_mgr
    self.__callbacks = {}

  def spawn(self, app):
    monitor_func = functools.partial(self._monitor_appliance, app)
    tornado.ioloop.IOLoop.instance().add_callback(monitor_func)
    cb = PeriodicCallback(monitor_func, self.APP_MONITOR_INTERVAL)
    self.__callbacks[app.id] = cb
    cb.start()

  def stop(self, app_id):
    cb = self.__callbacks.pop(app_id, None)
    if cb:
      cb.stop()

  async def _monitor_appliance(self, app):
    self.logger.info('Containers left: %s'%list(app.dag.parent_map.keys()))
    cb = self.__callbacks.get(app.id, None)
    if cb and cb.is_running and app.dag.is_empty:
      self.logger.info('DAG is empty, stop monitoring')
      cb.stop()
      self.__callbacks.pop(app.id, None)
      return
    free_contrs = [c.id for c in app.dag.get_free_containers()]
    self.logger.info('Launch free containers: %s'%free_contrs)
    for c in app.dag.get_free_containers():
      if c.state == ContainerState.SUBMITTED:
        status, _, err = await self.__contr_mgr.provision_container(c)
        if status not in (200, 409):
          self.logger.info(status)
          self.logger.error("Failed to launch container '%s'"%c)
          self.logger.error(err)
    self.logger.info('Update DAG')
    for c in app.dag.get_free_containers():
      _, c, err = await self.__contr_mgr.get_container(app.id, c.id)
      if err:
        self.logger.error(err)
        continue
      if (not app.dag.child_map.get(c.id, [])) \
          or (c.type == ContainerType.SERVICE and c.state == ContainerState.RUNNING) \
          or (c.type == ContainerType.JOB and c.state == ContainerState.SUCCESS):
        app.dag.remove_container(c.id)
      else:
        app.dag.update_container(c)
    status, msg, err = await self.__app_mgr.save_appliance(app, False)
    if err:
      self.logger.error(err)

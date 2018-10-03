import importlib
import schedule

from config import config
from commons import MongoClient, AutonomousMonitor
from commons import Manager, APIManager
from appliance.base import Appliance
from container.manager import ContainerManager
from schedule.local import ApplianceScheduleExecutor


class ApplianceManager(Manager):

  def __init__(self):
    self.__app_api = ApplianceAPIManager()
    self.__contr_mgr = ContainerManager()
    self.__app_db = ApplianceDBManager()

  async def get_appliance(self, app_id):
    status, app, err = await self.__app_db.get_appliance(app_id)
    if status != 200:
      return status, app, err
    app = Appliance(**app)
    status, app.containers, err = await self.__contr_mgr.get_containers(appliance=app_id)
    return 200, app, None

  async def get_appliances(self, **filters):
    return self.__app_db.get_appliances(**filters)

  async def create_appliance(self, data):
    status, _, _ = await self.get_appliance(data['id'])
    if status == 200:
      return 409, None, "Appliance '%s' already exists"%data['id']
    status, app, err = Appliance.parse(data)
    if err:
      self.logger.error(err)
      return status, None, err
    for c in app.containers:
      status, msg, err = await self.__contr_mgr.create_container(c.to_save())
      if err:
        self.logger.error(err)
        return status, None, err
    err = self._validate_appliance(app)
    if err:
      return err
    status, msg, err = await self.save_appliance(app, True)
    if err:
      self.logger.error(err)
      return status, None, err
    scheduler = self._get_scheduler(app.scheduler)
    self.logger.info('Appliance %s uses %s'%(app.id, scheduler.__class__.__name__))
    ApplianceScheduleExecutor(app.id, scheduler).start()
    return 201, app, None

  async def delete_appliance(self, app_id):
    status, app, err = await self.__app_db.get_appliance(app_id)
    if err:
      self.logger.error(err)
      return status, None, err
    self.logger.info("Stop monitoring appliance '%s'"%app_id)
    status, msg, err = await self.__contr_mgr.delete_containers(appliance=app_id)
    if err:
      self.logger.error(err)
      return 400, None, "Failed to deprovision jobs of appliance '%s'"%app_id
    self.logger.info(msg)
    status, msg, err = await self.__app_api.deprovision_appliance(app_id)
    if err and status != 404:
      self.logger.error(err)
      return 400, None, "Failed to deprovision appliance '%s'"%app_id
    ApplianceDeletionChecker(app_id).start()
    return status, msg, None

  async def save_appliance(self, app, upsert=True):
    return await self.__app_db.save_appliance(app, upsert)

  def _validate_appliance(self, app):

    def validate_dependencies(contrs):
      contrs = {c.id: c for c in contrs}
      parents = {}
      for c in contrs.values():
        nonexist = list(filter(lambda c: c not in contrs, c.dependencies))
        if nonexist:
          return 422, None, "Dependencies '%s' do not exist in this appliance"%nonexist
      parents.setdefault(c.id, set()).update(c.dependencies)
      for c, p in parents.items():
        cycles = ['%s<->%s'%(c, pp)
                  for pp in filter(lambda x: c in parents.get(x, set()), p)]
        if cycles:
          return 422, None, "Cycle(s) found: %s"%cycles

    return validate_dependencies(app.containers)

  def _get_scheduler(self, sched):
    try:
      sched_mod = '.'.join(sched.name.split('.')[:-1])
      sched_class = sched.name.split('.')[-1]
      return getattr(importlib.import_module(sched_mod), sched_class)(sched.config)
    except Exception as e:
      self.logger.error(str(e))
      return schedule.local.DefaultApplianceScheduler()


class ApplianceAPIManager(APIManager):

  def __init__(self):
    super(ApplianceAPIManager, self).__init__()

  async def get_appliance(self, app_id):
    api = config.marathon
    endpoint = '%s/groups/%s'%(api.endpoint, app_id)
    return await self.http_cli.get(api.host, api.port, endpoint)

  async def deprovision_appliance(self, app_id):
    api = config.marathon
    endpoint = '%s/groups/%s?force=true'%(api.endpoint, app_id)
    status, msg, err = await self.http_cli.delete(api.host, api.port, endpoint)
    if status not in (200, 404):
      return status, None, err
    return 200, "Services of appliance '%s' is being deprovisioned"%app_id, None


class ApplianceDBManager(Manager):

  def __init__(self):
    self.__app_col = MongoClient()[config.db.name].appliance

  async def get_appliances(self, **filters):
    return 200, [Appliance(**app) async for app in self.__app_col.find(filters)], None

  async def get_appliance(self, app_id):
    app = await self.__app_col.find_one(dict(id=app_id))
    if not app:
      return 404, None, "Appliance '%s' is not found"%app_id
    return 200, app, None

  async def save_appliance(self, app, upsert=True):
    await self.__app_col.replace_one(dict(id=app.id), app.to_save(), upsert=upsert)
    return 200, "Appliance '%s' has been saved"%app, None

  async def delete_appliance(self, app_id):
    await self.__app_col.delete_one(dict(id=app_id))
    return 200, "Appliance '%s' has been deleted"%app_id, None


class ApplianceDeletionChecker(AutonomousMonitor):

  def __init__(self, app_id):
    super(ApplianceDeletionChecker, self).__init__(3000)
    self.__app_id = app_id
    self.__app_api = ApplianceAPIManager()
    self.__app_db = ApplianceDBManager()
    self.__contr_mgr = ContainerManager()

  async def callback(self):
    status, _, err = await self.__app_api.get_appliance(self.__app_id)
    if status == 200:
      self.logger.info("Appliance '%s' still exists, deleting"%self.__app_id)
      await self.__app_api.deprovision_appliance(self.__app_id)
      return
    _, contrs, _ = await self.__contr_mgr.get_containers(appliance=self.__app_id)
    if contrs:
      self.logger.info("Found obsolete container(s) of appliance '%s', deleting"%self.__app_id)
      await self.__contr_mgr.delete_containers(appliance=self.__app_id)
      return
    if status == 404:
      self.logger.info("Delete appliance '%s' from database"%self.__app_id)
      await self.__app_db.delete_appliance(self.__app_id)
    elif status != 200:
      self.logger.error(err)
    self.stop()

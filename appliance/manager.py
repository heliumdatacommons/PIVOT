import sys
import importlib

from config import config
from commons import MotorClient, AutonomousMonitor
from commons import Manager, APIManager
from appliance.base import Appliance
from container.manager import ContainerManager
from scheduler.manager import ApplianceDAGDBManager


def get_scheduler():
  try:
    sched_mod = '.'.join(config.pivot.scheduler.split('.')[:-1])
    sched_class = config.pivot.scheduler.split('.')[-1]
    return getattr(importlib.import_module(sched_mod), sched_class)
  except Exception as e:
    sys.stderr.write(str(e) + '\n')
    from scheduler import DefaultApplianceScheduler
    return DefaultApplianceScheduler


class ApplianceManager(Manager):

  def __init__(self):
    self.__app_api = ApplianceAPIManager()
    self.__contr_mgr = ContainerManager()
    self.__app_db = ApplianceDBManager()
    self.__sched_class = get_scheduler()
    self.logger.info('Scheduler: %s'%self.__sched_class.__name__)

  async def get_appliance(self, app_id):
    status, app, err = await self.__app_db.get_appliance(app_id)
    if status != 200:
      return status, app, err
    app = Appliance(**app)
    status, app.containers, err = await self.__contr_mgr.get_containers(app_id)
    if not app.containers:
      self.logger.info("Empty appliance '%s', deleting"%app_id)
      status, msg, err = await self.delete_appliance(app_id)
      if err:
        self.logger.info(err)
      return 404, None, "Appliance '%s' is not found"%app_id
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
    scheduler = self.__sched_class(app)
    status, msg, err = await scheduler.initialize()
    if status != 200:
      self.logger.error(err)
      return status, msg, err
    scheduler.start()
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

  async def restore_appliance(self, app_id):
    raise NotImplemented


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
    self.__app_col = MotorClient().requester.appliance

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
    self.__dag_db = ApplianceDAGDBManager()

  async def callback(self):
    status, _, err = await self.__app_api.get_appliance(self.__app_id)
    if status == 200:
      self.logger.info("Appliance '%s' still exists, deleting"%self.__app_id)
      await self.__app_api.deprovision_appliance(self.__app_id)
      return
    if status == 404:
      self.logger.info("Delete appliance '%s' from database"%self.__app_id)
      await self.__app_db.delete_appliance(self.__app_id)
      await self.__dag_db.delete_appliance_dag(self.__app_id)
    elif status != 200:
      self.logger.error(err)
    self.stop()

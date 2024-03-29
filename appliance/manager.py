import importlib

import schedule
import volume

from tornado.gen import multi

from config import config
from commons import MongoClient, AutonomousMonitor
from commons import Manager, APIManager
from appliance import Appliance
from container.manager import ContainerManager
from volume.manager import VolumeManager
from schedule.local import ApplianceScheduleExecutor


class ApplianceManager(Manager):

  def __init__(self):
    self.__app_api = ApplianceAPIManager()
    self.__contr_mgr = ContainerManager()
    self.__app_db = ApplianceDBManager()
    self.__vol_mgr = VolumeManager()

  async def get_appliance(self, app_id):
    status, app, err = await self.__app_db.get_appliance(app_id)
    if status != 200:
      return status, app, err
    app = Appliance(**app)
    status, app.containers, err = await self.__contr_mgr.get_containers(appliance=app_id)
    if app.data_persistence:
      _, local_vols, _ = await self.__vol_mgr.get_local_volumes(appliance=app_id)
      _, global_vols, _ = await self.__vol_mgr.get_global_volumes_by_appliance(app_id)
      app.data_persistence.volumes = local_vols + global_vols
    if len(app.containers) == 0 \
        and (not app.data_persistence or len(app.data_persistence.volumes) == 0):
      await self.__app_db.delete_appliance(app_id)
      return 404, None, "Appliance '%s' is not found"%app_id
    return 200, app, None

  async def create_appliance(self, data):

    vol_mgr = self.__vol_mgr

    def validate_volume_mounts(app, vols_existed, vols_declared):
      all_vols = set(vols_existed) | set(vols_declared)
      vols_to_mount = set([pv.src for c in app.containers for pv in c.persistent_volumes])
      return list(vols_to_mount - all_vols)

    async def update_global_volumes(global_vols, app_id):
      for gpv in global_vols:
        gpv.subscribe(app_id)
      resps = await multi([vol_mgr.update_volume(gpv) for gpv in global_vols])
      for status, _, err in resps:
        if status != 200:
          self.logger.error(err)

    def set_container_volume_scope(contrs, vols):
      vols = {v.id: v for v in vols}
      for c in contrs:
        for pv in c.persistent_volumes:
          if pv.src in vols:
            pv.scope = vols[pv.src].scope

    status, app, _ = await self.get_appliance(data['id'])
    if status == 200 and len(app.containers) > 0:
      return 409, None, "Appliance '%s' already exists"%data['id']
    status, app, err = Appliance.parse(data)
    if status != 200:
      self.logger.error(err)
      return status, None, err

    # create persistent volumes if any
    dp = app.data_persistence
    if dp:
      resps = await multi([vol_mgr.get_local_volume(app.id, v.id) for v in dp.local_volumes]
                          + [vol_mgr.get_global_volume(v.id) for v in dp.global_volumes])
      vols_existed = set([v.id for status, v, _ in resps if status == 200])
      vols_declared = set([v.id for v in dp.volumes])
      invalid_vols = validate_volume_mounts(app, vols_existed, vols_declared)
      if len(invalid_vols) > 0:
        await self._clean_up_incomplete_appliance(app.id)
        return 400, None, 'Invalid persistent volume(s): %s'%invalid_vols
      global_vols = [v for _, v, _ in resps if v and v.scope == volume.VolumeScope.GLOBAL]
      if len(vols_existed) < len(dp.volumes):
        resps = await multi([vol_mgr.create_volume(v.to_save())
                             for v in dp.volumes
                             if v.id not in vols_existed])
        for status, v, err in resps:
          if status != 201:
            self.logger.error(err)
            await self._clean_up_incomplete_appliance(app.id)
            return status, None, err
          if v.scope == volume.VolumeScope.GLOBAL:
            global_vols += v,
      await update_global_volumes(global_vols, app.id)
      set_container_volume_scope(app.containers, dp.volumes)

    # create containers
    resps = await multi([self.__contr_mgr.create_container(c.to_save()) for c in app.containers])
    for status, _, err in resps:
      if status != 201:
        self.logger.error(err)
        await self._clean_up_incomplete_appliance(app.id)
        return status, None, err

    status, _, err = await self.save_appliance(app)
    if status != 200:
      self.logger.error(err)
      await self._clean_up_incomplete_appliance(app.id)
      return status, None, err
    scheduler = self._get_scheduler(app.scheduler)
    self.logger.info('Appliance %s uses %s'%(app.id, scheduler.__class__.__name__))
    ApplianceScheduleExecutor(app.id, scheduler).start()
    return 201, app, None

  async def delete_appliance(self, app_id, purge_data=False):
    vol_mgr = self.__vol_mgr
    self.logger.debug('Purge data?: %s' % purge_data)
    status, app, err = await self.get_appliance(app_id)
    if status != 200:
      self.logger.error(err)
      return status, None, err
    self.logger.info("Stop monitoring appliance '%s'"%app_id)

    # deprovision containers
    status, msg, err = await self.__contr_mgr.delete_containers(appliance=app_id)
    if status != 200:
      self.logger.error(err)
      return 207, None, "Failed to deprovision jobs of appliance '%s'"%app_id
    self.logger.info(msg)

    # deprovision/delete local persistent volumes if any
    if app.data_persistence:
      _, local_vols, _ = await vol_mgr.get_local_volumes(appliance=app_id)
      resps = await multi([(vol_mgr.purge_local_volume(app_id, v.id)
                            if purge_data else vol_mgr.deprovision_volume(v))
                           for v in local_vols])
      for i, (status, _, err) in enumerate(resps):
        if status != 200:
          self.logger.error(err)
          return 207, None, "Failed to deprovision persistent volume '%s'"%local_vols[i].id
      if purge_data:
        _, global_vols, _ = await vol_mgr.get_global_volumes_by_appliance(app_id)
        for gpv in global_vols:
          gpv.unsubscribe(app_id)
        for status, _, err in (await multi([vol_mgr.update_volume(gpv) for gpv in global_vols])):
          if status != 200:
            self.logger.error(err)

    # deprovision appliance
    status, msg, err = await self.__app_api.deprovision_appliance(app_id)
    if status != 200 and status != 404:
      self.logger.error(err)
      return 207, None, "Failed to deprovision appliance '%s'"%app_id
    if purge_data:
      await self.__app_db.delete_appliance(app_id)
    ApplianceDeletionChecker(app_id).start()
    return status, msg, None

  async def save_appliance(self, app, upsert=True):
    return await self.__app_db.save_appliance(app, upsert)

  def _get_scheduler(self, sched):
    try:
      sched_mod = '.'.join(sched.name.split('.')[:-1])
      sched_class = sched.name.split('.')[-1]
      return getattr(importlib.import_module(sched_mod), sched_class)(sched.config)
    except Exception as e:
      self.logger.error(str(e))
      return schedule.local.DefaultApplianceScheduler()

  async def _clean_up_incomplete_appliance(self, app_id):
    await self.__app_db.delete_appliance(app_id)
    await self.__contr_mgr.delete_containers(appliance=app_id)


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
    if status != 200:
      self.logger.error(err)
    self.stop()

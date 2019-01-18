import importlib

import schedule
import volume

from tornado.gen import multi

from config import config
from commons import MongoClient, AutonomousMonitor, Manager, APIManager
from appliance import Appliance
from container.manager import ContainerManager
from volume.manager import VolumeManager
from schedule.universal import GlobalSchedulerRunner
from schedule.local import ApplianceSchedulerRunner


class ApplianceManager(Manager):

  def __init__(self):
    self.__app_api = ApplianceAPIManager()
    self.__app_db = ApplianceDBManager()
    self.__contr_mgr = ContainerManager()
    self.__vol_mgr = VolumeManager()
    self.__global_sched = GlobalSchedulerRunner()

  async def get_appliance(self, app_id, full_blown=False):
    db, contr_mgr, vol_mgr = self.__app_db, self.__contr_mgr, self.__vol_mgr
    status, app, err = await db.get_appliance(app_id)
    if status != 200:
      return status, app, err
    app = Appliance(**app)
    status, app.containers, err = await contr_mgr.get_containers(appliance=app_id, full_blown=full_blown)
    if app.data_persistence:
      _, local_vols, _ = await  vol_mgr.get_local_volumes(appliance=app_id)
      _, global_vols, _ = await vol_mgr.get_global_volumes_by_appliance(app_id)
      app.data_persistence.volumes = local_vols + global_vols
    return 200, app, None

  async def create_appliance(self, data):
    db, vol_mgr, contr_mgr = self.__app_db, self.__vol_mgr, self.__contr_mgr

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

    # validation
    status, app, _ = await self.get_appliance(data['id'])
    if status == 200:
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
    resps = await multi([contr_mgr.create_container(c.to_save()) for c in app.containers])
    for status, _, err in resps:
      if status != 201:
        self.logger.error(err)
        await self._clean_up_incomplete_appliance(app.id)
        return status, None, err
    for c in app.containers:
      c.appliance = app
    await multi([contr_mgr.save_container(c, upsert=True)] for c in app.containers)
    status, _, err = await self.save_appliance(app)
    if status != 200:
      self.logger.error(err)
      await self._clean_up_incomplete_appliance(app.id)
      return status, None, err
    scheduler = self._get_scheduler(app.scheduler)
    self.logger.info('Appliance %s uses %s'%(app.id, scheduler.__class__.__name__))
    global_sched = self.__global_sched
    app_scheduler = ApplianceSchedulerRunner(app, scheduler)
    global_sched.register(app.id, app_scheduler)
    app_scheduler.start()
    return 201, app, None

  async def delete_appliance(self, app_id, purge_data=False):
    global_sched, contr_mgr, vol_mgr = self.__global_sched, self.__contr_mgr, self.__vol_mgr
    self.logger.debug('Purge data?: %s'%purge_data)
    status, app, err = await self.get_appliance(app_id)
    if status != 200:
      self.logger.error(err)
      return status, None, err
    app_sched = global_sched.deregister(app_id)
    if app_sched:
      self.logger.info("Stop monitoring appliance '%s'" % app_id)
      app_sched.stop()
    # deprovision containers
    status, msg, err = await contr_mgr.delete_containers(appliance=app_id)
    if status != 200:
      self.logger.error(err)
      return 207, None, "Failed to delete containers of appliance '%s'"%app_id
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
    if not app.data_persistence or purge_data:
      ApplianceDeletionEnforcer(app_id).start()
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

  async def get_deployments(self, app_id):
    api, endpoint = config.marathon, '%s/deployments'%config.marathon.endpoint
    status, deployments, err = await self.http_cli.get(api.host, api.port, endpoint)
    if status != 200:
      return status, None, err
    deployments = [d for d in deployments for affected in d['affectedApps']
                   if affected.startswith('/%s'%app_id)]
    return 200, deployments, None

  async def delete_appliance(self, app_id):
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


class ApplianceDeletionEnforcer(AutonomousMonitor):

  def __init__(self, app_id):
    super(ApplianceDeletionEnforcer, self).__init__(3000)
    self.__app_id = app_id
    self.__app_api = ApplianceAPIManager()
    self.__app_db = ApplianceDBManager()

  async def callback(self):
    app_id, api, db = self.__app_id, self.__app_api, self.__app_db
    status, deployments, err = await api.get_deployments(app_id)
    if status != 200:
      self.logger.error(err)
      return
    if len(deployments) > 0:
      self.logger.debug("Deprovisioning appliance [%s] (%d left)"%(app_id, len(deployments)))
      return
    self.logger.debug("Deleting appliance [%s]"%app_id)
    await multi([api.delete_appliance(app_id), db.delete_appliance(app_id)])
    self.stop()
    self.logger.debug("Appliance [%s] has been deleted, exit"%app_id)



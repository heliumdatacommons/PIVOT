import appliance.manager

from tornado.gen import multi

from config import config
from commons import MongoClient
from commons import APIManager, Manager
from volume import PersistentVolume, VolumeDeployment, VolumeScope
from locality import Placement


class VolumeManager(Manager):

  def __init__(self):
    self.__vol_api = VolumeAPIManager()
    self.__vol_db = VolumeDBManager()

  async def create_volume(self, data):
    """

    :param data: str, JSON string

    """
    status, vol, err = PersistentVolume.parse(data)
    if status != 200:
      return status, vol, err
    if vol.scope == VolumeScope.LOCAL:
      status, _, _ = await self.__vol_db.get_local_volume(vol.appliance, vol.id)
    elif vol.scope == VolumeScope.GLOBAL:
      status, _, _ = await self.__vol_db.get_global_volume(vol.id)
    if status == 200:
      return 409, None, "Volume '%s' already exists"%vol.id
    await self.__vol_db.save_volume(vol)
    return 201, vol, None

  async def update_volume(self, vol):
    """

    :param vol: volume.PersistentVolume

    """
    assert isinstance(vol, PersistentVolume)
    if vol.scope == VolumeScope.LOCAL:
      status, _, _ = await self.__vol_db.get_local_volume(vol.appliance, vol.id)
    elif vol.scope == VolumeScope.GLOBAL:
      status, _, _ = await self.__vol_db.get_global_volume(vol.id)
    if status != 200:
      return status, None, "Failed to update persistent volume '%s'"%vol.id
    await self.__vol_db.save_volume(vol, False)
    return status, "Persistent volume '%s' has been updated successfully"%vol.id, None

  async def provision_volume(self, vol):
    """

    :param vol: volume.PersistentVolume

    """
    assert isinstance(vol, PersistentVolume)
    status, _, err = await self.__vol_api.create_volume(vol)
    if status != 200:
      self.logger.error(err)
      return status, None, err
    vol.set_active()
    await self.__vol_db.save_volume(vol)
    return status, vol, None

  async def deprovision_volume(self, vol):
    """

    :param vol: volume.PersistentVolume

    """
    assert isinstance(vol, PersistentVolume)
    if vol.scope == VolumeScope.LOCAL:
      app_id = vol.appliance if isinstance(vol.appliance, str) else vol.appliance.id
      status, _, err = await self.__vol_api.delete_local_volume(app_id, vol.id)
    elif vol.scope == VolumeScope.GLOBAL:
      status, _, err = await self.__vol_api.delete_global_volume(vol.id)
    if status != 200:
      self.logger.error(err)
      return status, _, err
    vol.set_inactive()
    await self.__vol_db.save_volume(vol)
    return status, "Persistent volume '%s' has been deprovisioned"%vol.id, None

  async def purge_global_volume(self, vol_id):
    status, vol, err = await self.get_global_volume(vol_id)
    if status != 200:
      return status, None, err
    if len(vol.used_by) > 0:
      return 400, None, "Failed to delete the global persistent volume '%s': " \
                        "being used by appliance(s): %s"%(vol_id, vol.used_by)
    status, msg, err = await self.deprovision_volume(vol)
    if status != 200:
      return status, None, err
    status, msg, err = await self.__vol_api.delete_global_volume(vol_id, purge=True)
    if status != 200:
      return status, None, err
    await self.__vol_db.delete_volume(vol)
    return status, "Global persistent volume '%s' has been purged" % vol, None

  async def purge_local_volume(self, app_id, vol_id):
    status, vol, err = await self.get_local_volume(app_id, vol_id, full_blown=True)
    if status != 200:
      return status, None, err
    in_use = set([c.id for c in vol.appliance.containers
                  for v in c.persistent_volumes if v.src == vol_id])
    if len(in_use) > 0:
      return 400, None, "Failed to delete the local persistent volume '%s':  " \
                        "being used by container(s): %s"%(vol_id, list(in_use))
    status, msg, err = await self.__vol_api.delete_local_volume(app_id, vol_id, purge=True)
    if status != 200:
      return status, None, err
    await self.__vol_db.delete_volume(vol)
    return status, "Local persistent volume '%s' has been purged"%vol, None

  async def get_global_volume(self, vol_id):
    status, vol, err = await self._get_volume(self.__vol_db.get_global_volume,
                                              self.__vol_api.get_global_volume, vol_id)
    return (status, vol, None) if status == 200 else (status, None, err)

  async def get_local_volume(self, app_id, vol_id, full_blown=False):
    status, vol, err = await self._get_volume(self.__vol_db.get_local_volume,
                                              self.__vol_api.get_local_volume, app_id, vol_id)
    if status != 200:
      return status, None, err
    if full_blown:
      app_mgr = appliance.manager.ApplianceManager()
      _, vol.appliance, _ = await app_mgr.get_appliance(app_id)
    return status, vol, None

  async def get_global_volumes_by_appliance(self, app_id):
    return await self._get_volumes(VolumeScope.GLOBAL, used_by=app_id)

  async def get_global_volumes(self, **filters):
    return await self._get_volumes(VolumeScope.GLOBAL, **filters)

  async def get_local_volumes(self, full_blown=False, **filters):
    _, vols, _ = await self._get_volumes(VolumeScope.LOCAL, **filters)
    if full_blown:
      app_mgr = appliance.manager.ApplianceManager()
      resps = await multi([app_mgr.get_appliance(v.appliance) for v in vols])
      for i, (_, app, _) in enumerate(resps):
        vols[i] = app
    return 200, vols, None

  async def _get_volume(self, db_get_vol_func, api_get_vol_func, *args):
    resps = await multi([db_get_vol_func(*args), api_get_vol_func(*args)])
    for i, (status, output, err) in enumerate(resps):
      if status != 200:
        return status, None, err
      if i == 0:
        vol = output
      elif i == 1:
        vol.deployment = VolumeDeployment(placement=Placement(**output['placement']))
    return status, vol, None

  async def _get_volumes(self, scope, **filters):
    assert isinstance(scope, VolumeScope)
    filters.update(scope=scope.value)
    vols = await self.__vol_db.get_volumes(**filters)
    resps = await multi([self.__vol_api.get_local_volume(v.appliance, v.id)
                         if scope == VolumeScope.LOCAL else self.__vol_api.get_global_volume(v.id)
                         for v in vols])
    for i, (status, output, err) in enumerate(resps):
      if status != 200:
        self.logger.error(err)
        continue
      vols[i].deployment = VolumeDeployment(placement=Placement(**output['placement']))
    return 200, vols, None


class VolumeAPIManager(APIManager):

  def __init__(self):
    super(VolumeAPIManager, self).__init__()

  async def get_global_volume(self, vol_id):
    return await self._get_volume(str(vol_id))

  async def get_local_volume(self, app_id, vol_id):
    return await self._get_volume('%s-%s'%(app_id, vol_id))

  async def create_volume(self, vol):
    """

    :param vol: volume.Volume
    """
    api = config.ceph
    return await self.http_cli.post(api.host, api.port, '/fs', dict(vol.to_request()))

  async def delete_global_volume(self, vol_id, purge=False):
    return await self._delete_volume(str(vol_id), purge)

  async def delete_local_volume(self, app_id, vol_id, purge=False):
    return await self._delete_volume('%s-%s' % (app_id, vol_id), purge)

  async def _get_volume(self, ext_vol_id):
    api = config.ceph
    status, vol, err = await self.http_cli.get(api.host, api.port, '/fs/%s'%ext_vol_id)
    if status != 200:
      return status, None, err
    return status, vol, None

  async def _delete_volume(self, ext_vol_id, purge=False):
    api = config.ceph
    return await self.http_cli.delete(api.host, api.port, '/fs/%s?purge=%s' % (ext_vol_id, purge))


class VolumeDBManager(Manager):

  def __init__(self):
    self.__vol_col = MongoClient()[config.db.name].volume

  async def get_volumes(self, **filters):
    return [PersistentVolume.parse(v)[1] async for v in self.__vol_col.find(filters)]

  async def get_global_volume(self, vol_id):
    return await self._get_volume(id=vol_id)

  async def get_local_volume(self, app_id, vol_id):
    return await self._get_volume(id=vol_id, appliance=app_id)

  async def save_volume(self, vol, upsert=True):
    id = dict(id=vol.id)
    if vol.scope == VolumeScope.LOCAL:
      id.update(appliance=vol.appliance)
    await self.__vol_col.replace_one(id, vol.to_save(), upsert=upsert)

  async def delete_volume(self, vol):
    filters = dict(id=vol.id)
    if vol.scope == VolumeScope.LOCAL:
      app_id = vol.appliance if isinstance(vol.appliance, str) else vol.appliance.id
      filters.update(appliance=app_id)
    await self.__vol_col.delete_one(filters)
    return 200, "Volume '%s' has been deleted"%vol, None

  async def delete_volumes(self, **filters):
    await self.__vol_col.delete_many(filters)
    return 200, "Containers matching '%s' have been deleted"%filters, None

  async def _get_volume(self, **filters):
    vol = await self.__vol_col.find_one(filters)
    if not vol:
      return 404, None, "Volume matching '%s' is not found"%filters
    return 200, PersistentVolume.parse(vol)[1], None

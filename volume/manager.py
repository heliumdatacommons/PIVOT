import appliance.manager

from config import config
from commons import MongoClient
from commons import APIManager, Manager
from volume import PersistentVolume


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
    status, _, _ = await self.__vol_db.get_volume(vol.appliance, vol.id)
    if status == 200:
      return 409, None, "Volume '%s' already exists"%vol.id
    await self.__vol_db.save_volume(vol)
    return 201, vol, None

  async def provision_volume(self, vol):
    """

    :param vol: volume.PersistentVolume

    """
    assert isinstance(vol, PersistentVolume)
    status, _, err = await self.__vol_api.create_volume(vol)
    if status != 200:
      self.logger.error(err)
      return status, None, err
    vol.set_instantiated()
    await self.__vol_db.save_volume(vol)
    return status, vol, None

  async def deprovision_volume(self, vol):
    """

    :param vol: volume.PersistentVolume

    """
    assert isinstance(vol, PersistentVolume)
    app_id = vol.appliance if isinstance(vol.appliance, str) else vol.appliance.id
    status, _, err = await self.__vol_api.delete_volume(app_id, vol.id)
    if status != 200:
      self.logger.error(err)
      return status, _, err
    vol.unset_instantiated()
    await self.__vol_db.save_volume(vol)
    return status, "Persistent volume '%s' has been deprovisioned"%vol.id, None

  async def delete_volume(self, app_id, vol_id):
    status, vol, err = await self.get_volume(app_id, vol_id, full_blown=True)
    if status == 404:
      return status, vol, err
    in_use = set([c.id for c in vol.appliance.containers
                  for v in c.persistent_volumes if v.src == vol_id])
    if len(in_use) > 0:
      return 400, None, "Persistent volume '%s' is in use by container(s): %s"%(vol_id, list(in_use))
    status, msg, err = await self.__vol_api.delete_volume(app_id, vol_id, erasure=True)
    if status != 200:
      return status, None, err
    await self.__vol_db.delete_volume(vol)
    return status, "Persistent volume '%s' has been deleted"%vol, None

  async def get_volume(self, app_id, vol_id, full_blown=False):
    status, vol, err = await self.__vol_db.get_volume(app_id, vol_id)
    if status != 200:
      return status, vol, err
    if full_blown:
      app_mgr = appliance.manager.ApplianceManager()
      _, vol.appliance, _ = await app_mgr.get_appliance(app_id)
    return status, vol, None

  async def get_volumes(self, full_blown=False, **filters):
    vols = await self.__vol_db.get_volumes(**filters)
    if full_blown:
      app_mgr = appliance.manager.ApplianceManager()
      for v in vols:
        _, v.appliance, _ = await app_mgr.get_appliance(v.appliance)
    return 200, vols, None


class VolumeAPIManager(APIManager):

  def __init__(self):
    super(VolumeAPIManager, self).__init__()

  async def create_volume(self, vol):
    """

    :param vol: volume.Volume
    """
    api = config.ceph
    return await self.http_cli.post(api.host, api.port, '/fs', dict(vol.to_request()))

  async def delete_volume(self, app_id, vol_id, erasure=False):
    api = config.ceph
    return await self.http_cli.delete(api.host, api.port,
                                      '/fs/%s-%s?erasure=%s'%(app_id, vol_id, erasure))


class VolumeDBManager(Manager):

  def __init__(self):
    self.__vol_col = MongoClient()[config.db.name].volume

  async def get_volumes(self, **filters):
    return [PersistentVolume.parse(v)[1] async for v in self.__vol_col.find(filters)]

  async def get_volume(self, app_id, vol_id):
    return await self._get_volume(id=vol_id, appliance=app_id)

  async def save_volume(self, vol, upsert=True):
    await self.__vol_col.replace_one(dict(id=vol.id, appliance=vol.appliance),
                                     vol.to_save(), upsert=upsert)

  async def delete_volume(self, vol):
    app_id = vol.appliance if isinstance(vol.appliance, str) else vol.appliance.id
    await self.__vol_col.delete_one(dict(id=vol.id, appliance=app_id))
    return 200, "Volume '%s' has been deleted"%vol, None

  async def delete_volumes(self, **filters):
    await self.__vol_col.delete_many(filters)
    return 200, "Containers matching '%s' have been deleted"%filters, None

  async def _get_volume(self, **filters):
    vol = await self.__vol_col.find_one(filters)
    if not vol:
      return 404, None, "Volume matching '%s' is not found"%filters
    return 200, PersistentVolume(**vol), None

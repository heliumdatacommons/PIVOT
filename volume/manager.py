from tornado.escape import json_decode

from config import config
from commons import MongoClient
from commons import APIManager, Manager
from volume.base import Volume


class VolumeManager(Manager):

  def __init__(self):
    self.__vol_api = VolumeAPIManager()
    self.__vol_db = VolumeDBManager()

  async def create_volume(self, vol):
    status, vol, err = Volume.parse(vol)
    if status != 200:
      return status, None, err
    status, _, _ = await self.__vol_db.get_volume(vol.appliance, vol.id)
    if status == 200:
      return 409, None, "Volume '%s' already exists"%vol.id
    status, _, err = await self.__vol_api.create_volume(vol)
    if status != 200:
      return 409, None, json_decode(err)['err']
    await self.__vol_db.save_volume(vol)
    return status, vol, None

  async def erase_volume(self, app_id, vol_id):
    status, vol, err = await self.__vol_db.get_volume(app_id, vol_id)
    if status == 404:
      return status, vol, err
    status, msg, err = await self.__vol_api.erase_volume(app_id, vol_id)
    if status != 200:
      return status, None, err
    await self.__vol_db.delete_volume(vol)
    return status, "Volume '%s' has been deleted"%vol, None

  async def get_volume(self, app_id, vol_id):
    return await self.__vol_db.get_volume(app_id, vol_id)

  async def get_volumes(self, app_id):
    return await self.__vol_db.get_volumes(appliance=app_id)


class VolumeAPIManager(APIManager):

  def __init__(self):
    super(VolumeAPIManager, self).__init__()

  async def create_volume(self, vol):
    """

    :param vol: volume.base.Volume
    """
    api = config.ceph
    return await self.http_cli.post(api.host, api.port, '/fs', dict(vol.to_request()))

  async def erase_volume(self, app_id, vol_id):
    api = config.ceph
    return await self.http_cli.delete(api.host, api.port, '/fs/%s-%s'%(app_id, vol_id))


class VolumeDBManager(Manager):

  def __init__(self):
    self.__vol_col = MongoClient()[config.db.name].volume

  async def get_volumes(self, **filters):
    return [Volume.parse(v)[1] async for v in self.__vol_col.find(filters)]

  async def get_volume(self, app_id, vol_id):
    return await self._get_volume(id=vol_id, appliance=app_id)

  async def save_volume(self, vol, upsert=True):
    await self.__vol_col.replace_one(dict(id=vol.id, appliance=vol.appliance),
                                     vol.to_save(), upsert=upsert)

  async def delete_volume(self, vol):
    await self.__vol_col.delete_one(dict(id=vol.id, appliance=vol.appliance))
    return 200, "Volume '%s' has been deleted"%vol, None

  async def delete_volumes(self, **filters):
    await self.__vol_col.delete_many(filters)
    return 200, "Containers matching '%s' have been deleted"%filters, None

  async def _get_volume(self, **filters):
    vol = await self.__vol_col.find_one(filters)
    if not vol:
      return 404, None, "Volume matching '%s' is not found"%filters
    return Volume(**filters)

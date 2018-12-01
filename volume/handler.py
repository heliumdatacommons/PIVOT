import swagger

from tornado.gen import multi
from tornado.web import RequestHandler
from tornado.escape import json_encode

from volume.manager import VolumeManager
from commons import Loggable
from util import message, error


class ApplianceVolumesHandler(RequestHandler, Loggable):
  """
  ---
  - name: app_id
    required: true
    description: appliance ID
    type: str

  """

  def initialize(self):
    self.__vol_mgr = VolumeManager()

  @swagger.operation
  async def get(self, app_id):
    """
    Get persistent volumes in the requested appliance
    ---
    responses:
      200:
        description: volumes in the requested appliance
        content:
          application/json:
            schema:
              type: list
              items: PersistentVolume
      404:
        description: the requested appliance does not exist
        content:
          application/json:
            schema: Error

    """
    vol_mgr = self.__vol_mgr
    resps = await multi([vol_mgr.get_local_volumes(appliance=app_id),
                         vol_mgr.get_global_volumes_by_appliance(app_id)])
    volumes = []
    for status, vols, err in resps:
      if status != 200:
        self.set_status(status)
        self.write(error(err))
        return
      volumes += vols
    self.write(json_encode([v.to_render() for v in volumes]))


class ApplianceVolumeHandler(RequestHandler, Loggable):
  """
  ---
  - name: app_id
    required: true
    description: appliance ID
    type: str

  - name: vol_id
    required: true
    description: local persistent volume ID
    type: str

  """

  def initialize(self):
    self.__vol_mgr = VolumeManager()

  @swagger.operation
  async def get(self, app_id, vol_id):
    """
    Get local persistent volume
    ---
    responses:
      200:
        description: the requested volume is found and returned
        content:
          application/json:
            schema: PersistentVolume
      404:
        description: the requested volume is not found
        content:
          application/json:
            schema: Error

    """
    status, vol, err = await self.__vol_mgr.get_local_volume(app_id, vol_id)
    self.set_status(status)
    self.write(json_encode(vol.to_render() if status == 200 else error(err)))

  @swagger.operation
  async def delete(self, app_id, vol_id):
    """
    Delete local persistent volume
    ---
    responses:
      200:
        description: the requested local volume is purged
        content:
          application/json:
            schema: Message
      400:
        description: >
          the requested volume is in use by container(s) in the appliance and cannot be deleted yet
        content:
          application/json:
            schema: Error
      404:
        description: the requested volume is not found
        content:
          application/json:
            schema: Error

    """
    status, msg, err = await self.__vol_mgr.purge_local_volume(app_id, vol_id)
    self.set_status(status)
    self.write(json_encode(message(msg) if status == 200 else error(err)))


class GlobalVolumeHandler(RequestHandler, Loggable):
  """
  ---
  - name: vol_id
    required: true
    description: global persistent volume ID
    type: str

  """

  def initialize(self):
    self.__vol_mgr = VolumeManager()

  @swagger.operation
  async def get(self, vol_id):
    """
    Get global persistent volume
    ---
    responses:
      200:
        description: the requested global volume is found and returned
        content:
          application/json:
            schema: Message
      404:
        description: the requested global volume is not found
        content:
          application/json:
            schema: Error

    """
    status, vol, err = await self.__vol_mgr.get_global_volume(vol_id)
    self.set_status(status)
    self.write(vol.to_render() if status == 200 else error(err))

  @swagger.operation
  async def delete(self, vol_id):
    """
    Delete global persistent volume
    ---
    responses:
      200:
        description: the requested volume is purged
        content:
          application/json:
            schema: Message
      400:
        description: >
          the requested global volume is in use by container(s) in the appliance and cannot be
          purged yet
        content:
          application/json:
            schema: Error
      404:
        description: the requested volume is not found
        content:
          application/json:
            schema: Error

    """
    status, msg, err = await self.__vol_mgr.purge_global_volume(vol_id)
    self.set_status(status)
    self.write(json_encode(message(msg) if status == 200 else error(err)))


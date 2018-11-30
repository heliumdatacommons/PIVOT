import swagger

from tornado.web import RequestHandler
from tornado.escape import json_encode

from volume.manager import VolumeManager
from commons import Loggable
from util import message, error


class VolumesHandler(RequestHandler, Loggable):
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
              items: Volume
      404:
        description: the requested appliance does not exist
        content:
          application/json:
            schema: Error

    """
    status, vols, err = await self.__vol_mgr.get_local_volumes(appliance=app_id)
    self.set_status(status)
    self.write(json_encode([v.to_render() for v in vols] if status == 200 else error(err)))


class VolumeHandler(RequestHandler, Loggable):
  """
  ---
  - name: app_id
    required: true
    description: appliance ID
    type: str

  - name: vol_id
    required: true
    description: persistent volume ID
    type: str

  """

  def initialize(self):
    self.__vol_mgr = VolumeManager()

  @swagger.operation
  async def get(self, app_id, vol_id):
    """
    Get persistent volume
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
    Delete persistent volume
    ---
    responses:
      200:
        description: the requested volume is found and returned
        content:
          application/json:
            schema: PersistentVolume
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




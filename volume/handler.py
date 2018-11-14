import swagger

from tornado.web import RequestHandler
from tornado.escape import json_encode

from container.manager import ContainerManager
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
    pass

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
    pass



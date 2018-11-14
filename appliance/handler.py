import json
import swagger

from tornado.web import RequestHandler
from tornado.escape import json_decode

from appliance.manager import ApplianceManager
from container.manager import ContainerManager
from util import message, error
from commons import Loggable


class AppliancesHandler(RequestHandler, Loggable):

  def initialize(self):
    self.__app_mgr = ApplianceManager()
    self.__contr_mgr = ContainerManager()

  @swagger.operation
  async def post(self):
    """
    Create an appliance
    ---
    request_body:
      content:
        application/json:
          schema: Appliance
    responses:
      201:
        description: An appliance is created successfully
        content:
          application/json:
            schema: Appliance
      409:
        description: The appliance ID conflicts with an existing appliance
        content:
          application/json:
            schema: Error
      422:
        description: Appliance request is ill-formatted
        content:
          application/json:
            schema: Error
    """
    try:
      data = json_decode(self.request.body)
      status, app, err = await self.__app_mgr.create_appliance(data)
      self.set_status(status)
      self.write(json.dumps(app.to_render() if status == 201 else error(err)))
    except json.JSONDecodeError as e:
      self.set_status(422)
      self.write(error("Ill-formatted request: %s"%e))


class ApplianceHandler(RequestHandler, Loggable):
  """
  ---
  - name: app_id
    required: true
    description: appliance ID
    type: str
  """

  def initialize(self):
    self.__app_mgr = ApplianceManager()
    self.__contr_mgr = ContainerManager()

  @swagger.operation
  async def get(self, app_id):
    """
    Retrieve an appliance
    ---
    responses:
      200:
        description: The requested appliance is found
        content:
          application/json:
            schema: Appliance
      404:
        description: The requested appliance does not exist
        content:
          application/json:
            schema: Error
    """
    status, app, err = await self.__app_mgr.get_appliance(app_id)
    self.set_status(status)
    self.write(json.dumps(app.to_render() if status == 200 else error(err)))

  @swagger.operation
  async def delete(self, app_id):
    """
    Delete an appliance
    ---
    responses:
      200:
        description: The requested appliance is deleted successfully
        content:
          application/json:
            schema: Message
      207:
        description: Failed to delete the requested appliance due to failure in deleting
                     some, if not all, of its containers
        content:
          application/json:
            schema: Error
      404:
        description: The requested appliance does not exist
        content:
          application/json:
            schema: Error
    """
    status, msg, err = await self.__app_mgr.delete_appliance(app_id)
    self.set_status(status)
    self.write(json.dumps(message(msg) if status == 200 else error(err)))


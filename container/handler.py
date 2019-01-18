import swagger

from tornado.web import RequestHandler
from tornado.escape import json_encode

from container.manager import ContainerManager
from commons import Loggable
from util import message, error


class ServicesHandler(RequestHandler, Loggable):
  """
  ---
  - name: app_id
    required: true
    description: appliance ID
    type: str
  """

  def initialize(self):
    self.__contr_mgr = ContainerManager()

  @swagger.operation
  async def get(self, app_id):
    """
    Get services in the requested appliance
    ---
    responses:
      200:
        description: services in the requested appliance
        content:
          application/json:
            schema:
              type: list
              items: Service
      404:
        description: the requested appliance does not exist
        content:
          application/json:
            schema: Error
    """
    status, services, err = await self.__contr_mgr.get_containers(appliance=app_id,
                                                                  type='service')
    self.set_status(status)
    self.write(json_encode([s.to_render() for s in services] if status == 200 else error(err)))


class JobsHandler(RequestHandler, Loggable):
  """
  ---
  - name: app_id
    required: true
    description: appliance ID
    type: str
  """

  def initialize(self):
    self.__contr_mgr = ContainerManager()

  @swagger.operation
  async def get(self, app_id):
    """
    Get jobs in the requested appliance
    ---
    responses:
      200:
        description: jobs in the requested appliance
        content:
          application/json:
            schema:
              type: list
              items: Job
      404:
        description: the requested appliance does not exist
        content:
          application/json:
            schema: Error
    """
    status, services, err = await self.__contr_mgr.get_containers(appliance=app_id, type='job')
    self.set_status(status)
    self.write(json_encode([s.to_render() for s in services] if status == 200 else error(err)))

class ContainersHandler(RequestHandler, Loggable):

  def initialize(self):
    self.__contr_mgr = ContainerManager()


class ContainerHandler(RequestHandler, Loggable):
  """
  ---
  - name: app_id
    required: true
    description: appliance ID
    type: str
  - name: contr_id
    required: true
    description: container ID
    type: str
  """

  def initialize(self):
    self.__contr_mgr = ContainerManager()

  @swagger.operation
  async def get(self, app_id, contr_id):
    """
    Get a container
    ---
    responses:
      200:
        description: The requested container is found
        content:
          application/json:
            schema: Container
      404:
        description: The requested container is not found
        content:
          application/json:
            schema: Error
    """
    status, contr, err = await self.__contr_mgr.get_container(app_id, contr_id)
    self.set_status(status)
    self.write(json_encode(contr.to_render() if status == 200 else error(err)))

  @swagger.operation
  async def delete(self, app_id, contr_id):
    """
    Delete a container
    ---
    responses:
      200:
        description: The requested container is deleted successfully
        content:
          application/json:
            schema: Message
      404:
        description: The requested container is not found
        content:
          application/json:
            schema: Error
    """
    status, msg, err = await self.__contr_mgr.delete_container(app_id, contr_id)
    self.set_status(status)
    self.write(json_encode(message(msg) if status == 200 else error(err)))

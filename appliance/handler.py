import json
import tornado
import swagger

from tornado.web import RequestHandler

from appliance.base import Appliance
from appliance.manager import ApplianceManager
from container.manager import ContainerManager
from util import message, error
from util import Loggable


class AppliancesHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__app_mgr = ApplianceManager(config)
    self.__contr_mgr = ContainerManager(config)

  @swagger.operation
  async def post(self):
    try:
      data = tornado.escape.json_decode(self.request.body)
      status, app, err = Appliance.pre_check(data)
      if status != 200:
        self.set_status(status)
        self.write(error(err))
        return
      status, app, err = await self.__app_mgr.create_appliance(data)
      self.set_status(status)
      self.write(json.dumps(app.to_render() if status == 201 else error(err)))
    except json.JSONDecodeError as e:
      self.set_status(422)
      self.write(error("Ill-formatted request: %s"%e))


class ApplianceHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__app_mgr = ApplianceManager(config)
    self.__contr_mgr = ContainerManager(config)

  @swagger.operation
  async def get(self, app_id):
    status, app, err = await self.__app_mgr.get_appliance(app_id)
    self.set_status(status)
    self.write(json.dumps(app.to_render() if status == 200 else error(err)))

  @swagger.operation
  async def delete(self, app_id):
    status, msg, err = await self.__app_mgr.delete_appliance(app_id)
    self.set_status(status)
    self.write(json.dumps(message(msg) if status == 200 else error(err)))


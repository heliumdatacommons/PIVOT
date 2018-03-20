import json
import tornado

from tornado.web import RequestHandler

from appliance.manager import ApplianceManager
from container.manager import ContainerManager
from util import message, error
from util import Loggable


class AppliancesHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__app_mgr = ApplianceManager(config)
    self.__contr_mgr = ContainerManager(config)

  async def post(self):
    data = tornado.escape.json_decode(self.request.body)
    status, app, err = await self.__app_mgr.create_appliance(data)
    self.set_status(status)
    self.write(json.dumps(app.to_render() if status == 201 else error(err)))


class ApplianceHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__app_mgr = ApplianceManager(config)
    self.__contr_mgr = ContainerManager(config)

  async def get(self, app_id):
    status, app, err = await self.__app_mgr.get_appliance(app_id)
    self.set_status(status)
    self.write(json.dumps(app.to_render() if status == 200 else error(err)))

  async def delete(self, app_id):
    status, msg, err = await self.__app_mgr.delete_appliance(app_id)
    self.set_status(status)
    self.write(json.dumps(message(msg) if status == 200 else error(err)))


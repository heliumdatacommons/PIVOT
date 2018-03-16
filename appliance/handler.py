import json

from tornado.web import RequestHandler

from appliance.manager import ApplianceManager
from container.manager import ContainerManager
from util import Loggable


class AppliancesHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__app_mgr = ApplianceManager(config)
    self.__contr_mgr = ContainerManager(config)

  async def post(self):
    status, app = await self.__app_mgr.create_appliance(json.loads(self.request.body))
    self.set_status(status)
    self.write(json.dumps(app.to_render()) if status == 201 else app)
    self.finish()
    if status == 201:
      await self.__app_mgr.process_next_pending_container(app)


class ApplianceHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__app_mgr = ApplianceManager(config)
    self.__contr_mgr = ContainerManager(config)

  async def get(self, app_id):
    status, resp = await self.__app_mgr.get_appliance(app_id)
    self.set_status(status)
    self.write(resp)
    # self.write(json.dumps(resp.to_render()) if status == 200 else resp)

  async def delete(self, app_id):
    status, app, resp = await self.__app_mgr.delete_appliance(app_id)
    self.set_status(status)
    self.write(resp)
    if app:
      await self._deprovision_containers(app.containers)

  async def _deprovision_containers(self, contrs):
    for c in contrs:
      await self.__contr_mgr.deprovision_container(c)

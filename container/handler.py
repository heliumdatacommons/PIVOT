import json
import tornado

from tornado.web import RequestHandler

from container.manager import ContainerManager
from util import message, error
from util import Loggable


class ServicesHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__contr_mgr = ContainerManager(config)

  async def get(self, app_id):
    status, services, err = await self.__contr_mgr.get_containers(app_id, type='service')
    self.set_status(status)
    self.write(json.dumps([s.to_render() for s in services]
                          if status == 200 else error(err)))


class JobsHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__contr_mgr = ContainerManager(config)

  async def get(self, app_id):
    status, services, err = await self.__contr_mgr.get_containers(app_id, type='job')
    self.set_status(status)
    self.write(json.dumps([s.to_render() for s in services]
                          if status == 200 else error(err)))


class ContainersHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__contr_mgr = ContainerManager(config)

  async def post(self, app_id):
    data = tornado.escape.json_decode(self.request.body)
    data['appliance'] = app_id
    status, contr, err = await self.__contr_mgr.create_container(data)
    if status != 200:
      self.set_status(status)
      self.write(json.dumps(error(err)))
      return
    status, contr, err = await self.__contr_mgr.provision_container(contr)
    self.set_status(status)
    self.write(json.dumps(contr.to_render() if status == 200 else error(err)))


class ContainerHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__contr_mgr = ContainerManager(config)

  async def get(self, app_id, contr_id):
    status, contr, err = await self.__contr_mgr.get_container(app_id, contr_id)
    self.set_status(status)
    self.write(json.dumps(contr.to_render() if status == 200 else error(err)))

  async def delete(self, app_id, contr_id):
    status, msg, err = await self.__contr_mgr.delete_container(app_id, contr_id)
    self.set_status(status)
    self.write(json.dumps(message(msg) if status == 200 else error(err)))

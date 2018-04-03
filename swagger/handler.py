import json
from tornado.web import RequestHandler

from swagger import SwaggerAPIRegistry


class SwaggerUIHandler(RequestHandler):

  async def get(self):
    self.render('index.html')


class SwaggerAPIHandler(RequestHandler):

  def initialize(self):
    self.__api_reg = SwaggerAPIRegistry()
    self.__api_reg.register_operations(self.application)

  async def get(self):
    self.write(json.dumps(self.__api_reg.get_api_specs()))

import json
from tornado.web import RequestHandler

from swagger import SwaggerAPIRegistry


class SwaggerUIHandler(RequestHandler):

  async def get(self):
    self.render('index.html', url='/api')


class SwaggerAPIHandler(RequestHandler):

  def initialize(self):
    self.__api_reg = SwaggerAPIRegistry()

  async def get(self):
    self.write(json.dumps(self.__api_reg.get_api_specs()))

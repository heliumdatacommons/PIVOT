import json
import tornado
import multiprocessing as mp

from tornado.httpclient import AsyncHTTPClient
from tornado.web import Application, RequestHandler

from config import config
from util import MotorClient
from util import HTTP_METHOD_POST

from appliance import ApplianceManager, ApplianceMonitor
from container import ContainerManager


class AppliancesHandler(RequestHandler):

  def __init__(self, *args, **kwargs):
    super(AppliancesHandler, self).__init__(*args, **kwargs)
    self.__app_mgr = ApplianceManager()
    self.__contr_mgr = ContainerManager()

  async def post(self):
    status, app = await self.__app_mgr.create_appliance(json.loads(self.request.body))
    self.set_status(status)
    self.write(json.dumps(app.to_render()) if status == 201 else app)
    self.finish()
    if status == 201:
      await self.__app_mgr.process_next_pending_container(app)


class ApplianceHandler(RequestHandler):

  def __init__(self, *args, **kwargs):
    super(ApplianceHandler, self).__init__(*args, **kwargs)
    self.__app_mgr = ApplianceManager()
    self.__contr_mgr = ContainerManager()

  async def get(self, app_id):
    status, resp = await self.__app_mgr.get_appliance(app_id)
    self.set_status(status)
    self.write(json.dumps(resp.to_render()) if status == 200 else resp)

  async def delete(self, app_id):
    status, app, resp = await self.__app_mgr.delete_appliance(app_id)
    self.set_status(status)
    self.write(resp)
    if app:
      await self._deprovision_containers(app.containers)

  async def _deprovision_containers(self, contrs):
    for c in contrs:
      await self.__contr_mgr.deprovision_container(c)


class OffersHandler(RequestHandler):

  def __init__(self, *args, **kwargs):
    super(OffersHandler, self).__init__(*args, **kwargs)
    self.__app_mgr = ApplianceManager()
    self.__contr_mgr = ContainerManager()

  async def post(self, app_id, contr_id):
    offers = self._preprocess_offers(self.request.body)
    status, app, contr = await self.__app_mgr.accept_offer(app_id, contr_id, offers)
    self.set_status(status)
    self.finish()
    if status == 200:
      await self.__contr_mgr.provision_container(contr, app.network)
      await self.__app_mgr.process_next_pending_container(app)

  def _preprocess_offers(self, body):
    offers = json.loads(body.decode('utf-8'))
    offer_list = []
    for k, o in sorted(offers.items()):
      if not isinstance(o, dict):
        continue
      o['master'] = o.pop('Marathon', None)
      offer_list.append(o)
    return offer_list

def start_server():
  app = Application([
    (r'/appliance\/*', AppliancesHandler),
    (r'/appliance/([a-z0-9-]+\/*)', ApplianceHandler),
    (r'/appliance/([a-z0-9-]+\/*)/container/([a-z0-9-]+\/*)/offers', OffersHandler)
  ], **config)

  server = tornado.httpserver.HTTPServer(app)
  server.bind(app.settings['port'])
  server.start(app.settings['n_parallel'])
  tornado.ioloop.IOLoop.instance().start()


def start_container_monitor():
  ApplianceMonitor().start()
  tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
  pool = mp.Pool()
  pool.apply_async(start_container_monitor)
  pool.apply(start_server)



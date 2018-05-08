import tornado

from multiprocessing import Process
from tornado.web import Application, StaticFileHandler

from cluster.handler import ClusterInfoHandler
from appliance.handler import AppliancesHandler, ApplianceHandler
from appliance.ui.handler import ApplianceUIHandler
from container.handler import ContainersHandler, ContainerHandler, ServicesHandler, JobsHandler
from cluster.manager import ClusterManager
from ping.handler import PingHandler
from swagger.handler import SwaggerAPIHandler, SwaggerUIHandler
from config import config
from util import dirname


def start_server():
  app = Application([
    (r'/ping', PingHandler),
    (r'/cluster', ClusterInfoHandler),
    (r'/appliance', AppliancesHandler),
    (r'/appliance/([a-z0-9-]+\/*)', ApplianceHandler),
    (r'/appliance/([a-z0-9-]+\/*)/container',ContainersHandler),
    (r'/appliance/([a-z0-9-]+\/*)/service',ServicesHandler),
    (r'/appliance/([a-z0-9-]+\/*)/job',JobsHandler),
    (r'/appliance/([a-z0-9-]+\/*)/container/([a-z0-9-]+\/*)', ContainerHandler),
    (r'/appliance/([a-z0-9-]+\/*)/ui', ApplianceUIHandler),
    (r'/static/(.*)', StaticFileHandler, dict(path='%s/static'%dirname(__file__))),
    (r'/api', SwaggerAPIHandler),
    (r'/api/ui', SwaggerUIHandler),
  ])
  server = tornado.httpserver.HTTPServer(app)
  server.bind(config.pivot.port)
  server.start(config.pivot.n_parallel)
  tornado.ioloop.IOLoop.instance().start()


def start_cluster_monitor():
  tornado.ioloop.IOLoop.instance().run_sync(ClusterManager().monitor)


if __name__ == '__main__':
  p1 = Process(target=start_cluster_monitor)
  p2 = Process(target=start_server)
  p1.start()
  p2.start()
  p1.join()
  p2.join()




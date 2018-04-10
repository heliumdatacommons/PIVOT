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
from config import Configuration
from util import dirname


def start_server(config):
  app = Application([
    (r'/ping', PingHandler),
    (r'/cluster', ClusterInfoHandler, dict(config=config)),
    (r'/appliance', AppliancesHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)', ApplianceHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/container',ContainersHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/service',ServicesHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/job',JobsHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/container/([a-z0-9-]+\/*)', ContainerHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/ui', ApplianceUIHandler),
    (r'/static/(.*)', StaticFileHandler, dict(path='%s/static'%dirname(__file__))),
    (r'/api', SwaggerAPIHandler),
    (r'/api/ui', SwaggerUIHandler),
  ])
  server = tornado.httpserver.HTTPServer(app)
  server.bind(config.pivot.port)
  server.start(config.pivot.n_parallel)
  tornado.ioloop.IOLoop.instance().start()


def start_cluster_monitor(config):
  tornado.ioloop.IOLoop.instance().run_sync(ClusterManager(config).monitor)


if __name__ == '__main__':
  config = Configuration.read_config('%s/config.yml'%dirname(__file__))
  p1 = Process(target=start_cluster_monitor, args=(config, ))
  p2 = Process(target=start_server, args=(config, ))
  p1.start()
  p2.start()
  p1.join()
  p2.join()




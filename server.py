import tornado

from tornado.web import Application, StaticFileHandler
from tornado.httpserver import HTTPServer

from cluster.handler import ClusterInfoHandler
from cluster.manager import ClusterManager
from appliance import Appliance
from appliance.handler import AppliancesHandler, ApplianceHandler
from appliance.ui.handler import ApplianceUIHandler
from container import Container
from container.handler import ContainersHandler, ContainerHandler, ServicesHandler, JobsHandler
from volume import PersistentVolume
from volume.handler import ApplianceVolumesHandler, ApplianceVolumeHandler, GlobalVolumeHandler
from schedule.universal import GlobalSchedulerRunner
from index.handler import IndexHandler
from ping.handler import PingHandler
from swagger.handler import SwaggerAPIHandler, SwaggerUIHandler
from config import config
from util import dirname


def start_cluster_monitor():
  tornado.ioloop.IOLoop.instance().add_callback(ClusterManager().start_monitor)


def start_global_scheduler():
  tornado.ioloop.IOLoop.instance().add_callback(GlobalSchedulerRunner().start)


def start_server():
  app = Application([
    (r'\/*', IndexHandler),
    (r'/ping\/*', PingHandler),
    (r'/cluster\/*', ClusterInfoHandler),
    (r'/appliance\/*', AppliancesHandler),
    (r'/appliance/(%s)\/*'%Appliance.ID_PATTERN, ApplianceHandler),
    (r'/appliance/(%s)/container\/*'%Appliance.ID_PATTERN, ContainersHandler),
    (r'/appliance/(%s)/volume\/*' % Appliance.ID_PATTERN, ApplianceVolumesHandler),
    (r'/appliance/(%s)/service\/*'%Appliance.ID_PATTERN, ServicesHandler),
    (r'/appliance/(%s)/job\/*'%Container.ID_PATTERN, JobsHandler),
    (r'/appliance/(%s)/ui\/*'%Appliance.ID_PATTERN, ApplianceUIHandler),
    (r'/appliance/(%s)/container/(%s)\/*'%(Appliance.ID_PATTERN,
                                           Container.ID_PATTERN), ContainerHandler),
    (r'/appliance/(%s)/volume/(%s)\/*'%(Appliance.ID_PATTERN,
                                        PersistentVolume.ID_PATTERN), ApplianceVolumeHandler),
    (r'/volume/(%s)\/*'%PersistentVolume.ID_PATTERN, GlobalVolumeHandler),
    (r'/static/(.*)', StaticFileHandler, dict(path='%s/static'%dirname(__file__))),
    (r'/api', SwaggerAPIHandler),
    (r'/api/ui', SwaggerUIHandler),
  ])
  ssl_options = None
  if config.pivot.https:
    ssl_options = dict(certfile='/etc/pivot/server.pem', keyfile='/etc/pivot/server.key')
  server = tornado.httpserver.HTTPServer(app, ssl_options=ssl_options)
  server.bind(config.pivot.port)
  server.start(config.pivot.n_parallel)
  start_cluster_monitor()
  start_global_scheduler()
  tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
  start_server()




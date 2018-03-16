import json
import tornado
import multiprocessing as mp

from tornado.web import Application
from cluster.handler import ClusterInfoHandler
from appliance.handler import AppliancesHandler, ApplianceHandler
from container.handler import ContainersHandler, ContainerHandler, ServicesHandler, JobsHandler
from cluster.base import Cluster


def load_config(cfg_file_path):
  return json.load(open(cfg_file_path))


def start_server():
  config = load_config('config.json')
  cluster = Cluster(config)
  tornado.ioloop.IOLoop.instance().run_sync(cluster.monitor)
  app = Application([
    (r'/cluster', ClusterInfoHandler, dict(config=config)),
    (r'/appliance\/*', AppliancesHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)', ApplianceHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/container',ContainersHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/service',ServicesHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/job',JobsHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/container/([a-z0-9-]+\/*)', ContainerHandler, dict(config=config)),
  ], **config)

  server = tornado.httpserver.HTTPServer(app)
  server.bind(app.settings['port'])
  server.start(app.settings['n_parallel'])
  tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
  pool = mp.Pool()
  pool.apply(start_server)




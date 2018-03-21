import json
import tornado

from tornado.web import Application, StaticFileHandler

from cluster.handler import ClusterInfoHandler
from appliance.handler import AppliancesHandler, ApplianceHandler
from appliance.ui.handler import ApplianceUIHandler
from container.handler import ContainersHandler, ContainerHandler, ServicesHandler, JobsHandler
from cluster.base import Cluster
from util import Config, DCOSConfig, URLMap


def load_config(cfg_file_path):
  cfg = json.load(open(cfg_file_path))

  cfg['url'] = URLMap(**{k: '%s/%s'%(cfg['dcos']['master_url'], cfg['dcos']['%s_endpoint'%k])
                         for k in ['service_scheduler', 'job_scheduler', 'mesos_master']})
  cfg['dcos'] = DCOSConfig(**cfg['dcos'])
  return Config(**cfg)


def start_server():
  config = load_config('config.json')
  cluster = Cluster(config)
  tornado.ioloop.IOLoop.instance().run_sync(cluster.monitor)
  app = Application([
    (r'/cluster', ClusterInfoHandler, dict(config=config)),
    (r'/appliance', AppliancesHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)', ApplianceHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/container',ContainersHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/service',ServicesHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/job',JobsHandler, dict(config=config)),
    (r'/appliance/([a-z0-9-]+\/*)/container/([a-z0-9-]+\/*)', ContainerHandler, dict(config=config)),
    (r'/ui/appliance/([a-z0-9-]+\/*)', ApplianceUIHandler),
    (r'/static/(.*)', StaticFileHandler, dict(path='./static')),
  ])
  server = tornado.httpserver.HTTPServer(app)
  server.bind(config.port)
  server.start(config.n_parallel)
  tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
  start_server()




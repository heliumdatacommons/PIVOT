import json

from tornado.web import RequestHandler

from cluster.base import Cluster
from util import Loggable


class ClusterInfoHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__cluster = Cluster(config)

  def get(self):
    self.write(json.dumps([h.to_render() for h in self.__cluster.hosts]))

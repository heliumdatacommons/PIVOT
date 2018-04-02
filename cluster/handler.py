import json
import swagger

from tornado.web import RequestHandler

from cluster.manager import ClusterManager
from util import Loggable


class ClusterInfoHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__cluster_mgr = ClusterManager(config)

  async def get(self):
    cluster = await self.__cluster_mgr.get_cluster()
    self.write(json.dumps(cluster.to_render()))

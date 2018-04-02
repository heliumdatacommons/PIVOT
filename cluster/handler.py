import json
import swagger

from tornado.web import RequestHandler

from cluster.manager import ClusterManager
from util import Loggable


class ClusterInfoHandler(RequestHandler, Loggable):

  def initialize(self, config):
    self.__cluster_mgr = ClusterManager(config)

  @swagger.operation
  async def get(self):
    """
    Get cluster updates
    ---
    responses:
      200:
        description: Updates on hosts in the cluster
        content:
          application/json:
            schema:
              type: list
              items: Host
    """
    hosts = await self.__cluster_mgr.get_cluster()
    self.write(json.dumps([h.to_render() for h in hosts]))

import json
import swagger

from tornado.web import RequestHandler

from cluster.manager import ClusterManager
from commons import Loggable


class ClusterInfoHandler(RequestHandler, Loggable):

  def initialize(self):
    self.__cluster_mgr = ClusterManager()

  @swagger.operation
  async def get(self):
    """
    Get cluster updates
    ---
    parameters:
      - name: hostname
        description: Hostname of the physical host
        in: query
        type: list
        items: str
        example: 10.52.0.107
      - name: public_ip
        description: Public IP address of the physical host
        in: query
        type: list
        items: str
        example: 129.114.109.33
    responses:
      200:
        description: Updates on hosts in the cluster
        content:
          application/json:
            schema:
              type: list
              items: Host
    """
    if self.request.query_arguments:
      args = {k: [v.decode('utf-8') for v in vals]
              for k, vals in self.request.query_arguments.items()}
      hosts = await self.__cluster_mgr.find_agents(**args)
      self.write(json.dumps([h.to_render() for h in hosts]))
    else:
      hosts = await self.__cluster_mgr.get_cluster()
      self.write(json.dumps([h.to_render() for h in hosts]))

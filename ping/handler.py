import swagger

from tornado.web import RequestHandler

from util import Loggable


class PingHandler(RequestHandler, Loggable):

  @swagger.operation
  async def get(self):
    """
    Check whether the service is up and running
    ---
    responses:
      200:
        content:
          text/plain:
            schema:
              type: str
    """
    self.write('Pong!')

import swagger

from tornado.web import RequestHandler


@swagger.model
class ApplianceUIHandler(RequestHandler):
  """
  ---
  - name: app_id
    required: true
    description: appliance ID
    type: str
  """

  @swagger.operation
  async def get(self, app_id):
    """
    Get visual of the appliance's current status
    ---
    responses:
      200:
        description: Visual of the appliance's current status
        content:
          text/html:
            schema:
              type: file

    """
    self.render('appliance.html', appliance=app_id)

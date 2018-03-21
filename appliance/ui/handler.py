from tornado.web import RequestHandler


class ApplianceUIHandler(RequestHandler):

  def get(self, app_id):
    self.render('appliance.html', appliance=app_id)

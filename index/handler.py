from tornado.web import RequestHandler


class IndexHandler(RequestHandler):

  def get(self):
    self.redirect('/api/ui')



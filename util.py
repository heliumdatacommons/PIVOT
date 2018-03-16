import sys
import json
import motor
import logging

from tornado.httpclient import AsyncHTTPClient, HTTPError


def message(msg):
  return dict(message=msg)

def error(msg):
  return dict(error=msg)


class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class MotorClient(motor.motor_tornado.MotorClient, metaclass=Singleton):

  def __init__(self, *args, **kargs):
    super(MotorClient, self).__init__(*args, **kargs)


class Loggable(object):

  @property
  def logger(self):
    fmt = logging.Formatter('%(asctime)s|%(levelname)s|%(process)d|%(name)s.%(funcName)s'
                            '::%(lineno)s\t%(message)s')
    logger = logging.getLogger(self.__class__.__name__)
    logger.setLevel(logging.INFO)
    if not logger.hasHandlers():
      stream_hdlr = logging.StreamHandler(sys.stdout)
      stream_hdlr.setFormatter(fmt)
      file_hdlr = logging.FileHandler('pivot.log')
      file_hdlr.setFormatter(fmt)
      logger.addHandler(stream_hdlr)
      logger.addHandler(file_hdlr)
    return logger


class SecureAsyncHttpClient(Loggable):

  def __init__(self, config):
    self.__config = dict(config)
    self.__cli = AsyncHTTPClient()
    self.__headers = {
      'Content-Type': 'application/json',
      'Authorization': 'token=%s'%self._read_auth_token()
    }

  async def get(self, url, **headers):
    return await self._fetch(url, 'GET', None, **headers)

  async def post(self, url, body, **headers):
    return await self._fetch(url, 'POST', body, **headers)

  async def put(self, url, body, **headers):
    return await self._fetch(url, 'PUT', body, **headers)

  async def delete(self, url, body=None, **headers):
    return await self._fetch(url, 'DELETE', body, **headers)

  async def _fetch(self, url, method, body, **headers):
    try:
      if isinstance(body, dict):
        body = json.dumps(body)
      r = await self.__cli.fetch(url, method=method, body=body,
                                 headers=dict(**self.__headers, **headers))
      body = r.body.decode('utf-8')
      if body:
        body = json.loads(body)
      return 200, body, None
    except json.JSONDecodeError as de:
      return 422, None, error(de.msg)
    except HTTPError as e:
      return e.response.code, None, error(e.response.body.decode('utf-8'))

  def _read_auth_token(self):
    with open(self.__config['dcos']['token_file_path']) as f:
      return f.readline().strip('\n')

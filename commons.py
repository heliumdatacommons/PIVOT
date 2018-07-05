import sys
import json
import time
import logging
import tornado

from abc import ABCMeta, abstractmethod
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.ioloop import PeriodicCallback
from motor.motor_tornado import MotorClient

from util import error, dirname
from config import config


class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
          cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Loggable(object):

  @property
  def logger(self):
    fmt = logging.Formatter('%(asctime)s|%(levelname)s|%(process)d|%(name)s.%(funcName)s'
                            '::%(lineno)s\t%(message)s')
    logger = logging.getLogger(self.__class__.__name__)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
      stream_hdlr = logging.StreamHandler(sys.stdout)
      stream_hdlr.setFormatter(fmt)
      file_hdlr = logging.FileHandler('%s/log/pivot.log'%dirname(__file__))
      file_hdlr.setFormatter(fmt)
      logger.addHandler(stream_hdlr)
      logger.addHandler(file_hdlr)
    return logger


class MongoClient(MotorClient, metaclass=Singleton):

  def __init__(self, *args, **kwargs):
    super(MongoClient, self).__init__(config.db.host, config.db.port, *args, **kwargs)


class AsyncHttpClientWrapper(Loggable):

  def __init__(self):
    self.__cli = AsyncHTTPClient()
    self.__headers = {'Content-Type': 'application/json'}

  async def get(self, host, port, endpoint, is_https=False, **headers):
    return await self._fetch(host, port, endpoint, 'GET', None, is_https, **headers)

  async def post(self, host, port, endpoint, body, is_https=False, **headers):
    return await self._fetch(host, port, endpoint, 'POST', body, is_https, **headers)

  async def put(self, host, port, endpoint, body, is_https=False, **headers):
    return await self._fetch(host, port, endpoint, 'PUT', body, is_https, **headers)

  async def delete(self, host, port, endpoint, body=None, is_https=False, **headers):
    return await self._fetch(host, port, endpoint, 'DELETE', body, is_https, **headers)

  async def _fetch(self, host, port, endpoint, method, body, is_https=False, **headers):
    protocol = 'https' if is_https else 'http'
    try:
      if isinstance(body, dict):
        body = json.dumps(body)
      r = await self.__cli.fetch('%s://%s:%d%s'%(protocol, host, port, endpoint),
                                 method=method, body=body,
                                 headers=dict(**self.__headers, **headers))
      body = r.body.decode('utf-8')
      if body:
        body = json.loads(body)
      return 200, body, None
    except json.JSONDecodeError as de:
      return 422, None, error(de.msg)
    except HTTPError as e:
      if e.code == 599:
        return e.code, None, e.message
      return e.code, None, error(e.response.body.decode('utf-8'))
    except (ConnectionRefusedError, ConnectionResetError):
      self.logger.warning('Connection refused/reset, retry after 3 seconds')
      time.sleep(3)
      return await self._fetch(host, port, endpoint, method, body, is_https, **headers)


class Manager(Loggable, metaclass=Singleton):

  def __init__(self): pass


class APIManager(Manager):

  def __init__(self):
    self.http_cli = AsyncHttpClientWrapper()


class AutonomousMonitor(Loggable, metaclass=ABCMeta):

  def __init__(self, interval):
    self.__interval = interval
    self.__cb = None

  @property
  def is_running(self):
    return self.__cb and self.__cb.is_running

  def start(self):
    tornado.ioloop.IOLoop.instance().add_callback(self.callback)
    self.__cb = PeriodicCallback(self.callback, self.__interval)
    self.__cb.start()

  def stop(self):
    self.__cb.stop()

  @abstractmethod
  async def callback(self):
    raise NotImplemented

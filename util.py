import os
import re
import sys
import json
import motor
import logging
import subprocess

from tornado.httpclient import AsyncHTTPClient, HTTPError


def dirname(f):
  return os.path.dirname(os.path.abspath(f))


def message(msg):
  return dict(message=msg)


def error(msg):
  return dict(error=msg)


def parse_container_short_id(p, appliance):
  return re.sub(r'(.*)\@([a-z0-9\.-]+)(.*)',
                r'\1\2-%s.marathon.containerip.dcos.thisdcos.directory\3'%appliance,
                str(p))


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
    if not logger.handlers:
      stream_hdlr = logging.StreamHandler(sys.stdout)
      stream_hdlr.setFormatter(fmt)
      file_hdlr = logging.FileHandler('%s/log/pivot.log'%dirname(__file__))
      file_hdlr.setFormatter(fmt)
      logger.addHandler(stream_hdlr)
      logger.addHandler(file_hdlr)
    return logger


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
      return e.response.code, None, error(e.response.body.decode('utf-8'))

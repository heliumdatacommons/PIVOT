import os
import re
import sys
import json
import motor
import logging
import subprocess

from collections import namedtuple
from tornado.httpclient import AsyncHTTPClient, HTTPError


Config = namedtuple('Config', 'dcos port n_parallel url')
DCOSConfig = namedtuple('DCOSConfig', 'token_file_path master_url mesos_master_endpoint '
                                      'service_scheduler_endpoint job_scheduler_endpoint')
URLMap = namedtuple('URLMap', 'mesos_master service_scheduler job_scheduler')


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


class SecureAsyncHttpClient(Loggable):

  def __init__(self, config):
    self.__config = config
    self.__cli = AsyncHTTPClient()
    self.__headers = {
      'Content-Type': 'application/json',
      'Authorization': 'token=%s'%self._get_auth_token()
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

  def _get_auth_token(self):
    try:
      out = subprocess.check_output('dcos config show core.dcos_acs_token', shell=True)
      return out.decode('utf-8').strip('\n')
    except subprocess.CalledProcessError as e:
      self.logger.error(e)
      raise Exception('DC/OS is not properly set up and authenticated')

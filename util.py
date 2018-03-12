import sys
import json
import motor
import logging

HTTP_METHOD_GET='GET'
HTTP_METHOD_POST='POST'
HTTP_METHOD_PUT = 'PUT'
HTTP_METHOD_DELETE = 'DELETE'

APPLICATION_JSON = {'Content-Type': 'application/json'}


def message(msg):
  return json.dumps(dict(message=msg))


def error(msg):
  return json.dumps(dict(error=msg))


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
      file_hdlr = logging.FileHandler('app_requester.log')
      file_hdlr.setFormatter(fmt)
      logger.addHandler(stream_hdlr)
      logger.addHandler(file_hdlr)
    return logger





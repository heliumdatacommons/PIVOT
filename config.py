import sys
import yaml

from util import dirname


class API:

  def __init__(self, port, endpoint, host=None, *args, **kwargs):
    self.__host = host
    self.__port = int(port)
    self.__endpoint = endpoint

  @property
  def host(self):
    return self.__host

  @property
  def port(self):
    return self.__port

  @property
  def endpoint(self):
    return self.__endpoint

  @host.setter
  def host(self, h):
    self.__host = h

  @port.setter
  def port(self, p):
    self.__port = int(p)


class MesosAPI(API):
  
  def __init__(self, port=5050, *args, **kwargs):
    kwargs.update(port=port, endpoint='')
    super(MesosAPI, self).__init__(*args, **kwargs)


class MarathonAPI(API):

  def __init__(self, port=8080, *args, **kwargs):
    kwargs.update(port=port, endpoint='/v2')
    super(MarathonAPI, self).__init__(*args, **kwargs)


class ChronosAPI(API):

  def __init__(self, port=9090, *args, **kwargs):
    kwargs.update(port=port, endpoint='/v1/scheduler')
    super(ChronosAPI, self).__init__(*args, **kwargs)


class ExhibitorAPI(API):

  def __init__(self, port=8181, *args, **kwargs):
    kwargs.update(port=port, endpoint='/exhibitor/v1')
    super(ExhibitorAPI, self).__init__(*args, **kwargs)


class iRODSAPI(API):

  def __init__(self, port=0, *args, **kwargs):
    kwargs.update(port=port, endpoint='/v1')
    super(iRODSAPI, self).__init__(*args, **kwargs)


class GeneralConfig:

  def __init__(self, master, port=9090, n_parallel=1,
               scheduler='scheduler.DefaultApplianceScheduler', ha=False,
               *args, **kwargs):
    self.__master = master
    self.__port = port
    self.__n_parallel = n_parallel
    self.__scheduler = scheduler
    self.__ha = ha

  @property
  def master(self):
    return self.__master

  @property
  def port(self):
    return self.__port

  @property
  def n_parallel(self):
    return self.__n_parallel

  @master.setter
  def master(self, master):
    self.__master = master

  @property
  def scheduler(self):
    return self.__scheduler

  @property
  def ha(self):
    return self.__ha


class DatabaseConfig:

  def __init__(self, host, port, *args, **kwargs):
    self.__host = host
    self.__port = port

  @property
  def host(self):
    return self.__host

  @property
  def port(self):
    return self.__port


class Configuration:

  @classmethod
  def read_config(cls, cfg_file_path):
    cfg = yaml.load(open(cfg_file_path))
    try:
      pivot_cfg = GeneralConfig(**cfg.get('pivot', {}))
    except Exception as e:
      sys.stderr.write(str(e))
      sys.stderr.write('PIVOT configuration is not set correctly\n')
      sys.exit(1)
    try:
      db_cfg = DatabaseConfig(**cfg.get('db', {}))
    except Exception as e:
      sys.stderr.write(str(e))
      sys.stderr.write('Database configuration is not set correctly\n')
      sys.exit(2)
    return Configuration(pivot=pivot_cfg,
                         db=db_cfg,
                         mesos=MesosAPI(**cfg.get('mesos', {})),
                         marathon=MarathonAPI(**cfg.get('marathon', {})),
                         chronos=ChronosAPI(**cfg.get('chronos', {})),
                         exhibitor=ExhibitorAPI(**cfg.get('exhibitor', {})),
                         irods=iRODSAPI(**cfg.get('irods', {})))

  def __init__(self, pivot, db, mesos=None, marathon=None, chronos=None,
               exhibitor=None, irods=None, *args, **kwargs):
    self.__pivot = pivot
    self.__db = db
    self.__mesos = mesos
    self.__marathon = marathon
    self.__chronos = chronos
    self.__exhibitor = exhibitor
    self.__irods = irods

  @property
  def pivot(self):
    return self.__pivot

  @property
  def db(self):
    return self.__db

  @property
  def mesos(self):
    return self.__mesos

  @property
  def marathon(self):
    return self.__marathon

  @property
  def chronos(self):
    return self.__chronos

  @property
  def exhibitor(self):
    return self.__exhibitor

  @property
  def irods(self):
    return self.__irods


config = Configuration.read_config('%s/config.yml'%dirname(__file__))

import yaml


class API:

  def __init__(self, host, port, endpoint, *args, **kwargs):
    self.__host = host
    self.__port = port
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


class MesosAPI(API):
  
  def __init__(self, host, port=5050, *args, **kwargs):
    kwargs.update(host=host, port=port, endpoint='/')
    super(MesosAPI, self).__init__(*args, **kwargs)


class MarathonAPI(API):

  def __init__(self, host, port=8080, *args, **kwargs):
    kwargs.update(host=host, port=port, endpoint='/v2')
    super(MarathonAPI, self).__init__(*args, **kwargs)


class ChronosAPI(API):

  def __init__(self, host, port=9090, *args, **kwargs):
    kwargs.update(host=host, port=port, endpoint='/v1/scheduler')
    super(ChronosAPI, self).__init__(*args, **kwargs)


class GeneralConfig:

  def __init__(self, port=9090, n_parallel=1, *args, **kwargs):
    self.__port = port
    self.__n_parallel = n_parallel

  @property
  def port(self):
    return self.__port

  @property
  def n_parallel(self):
    return self.__n_parallel


class Configuration:

  @classmethod
  def read_config(cls, cfg_file_path):
    cfg = yaml.load(open(cfg_file_path))
    return Configuration(pivot=GeneralConfig(**cfg.get('pivot', {})),
                         mesos=MesosAPI(**cfg.get('mesos', {})),
                         marathon=MarathonAPI(**cfg.get('marathon', {})),
                         chronos=ChronosAPI(**cfg.get('chronos', {})))

  def __init__(self, pivot, mesos, marathon, chronos):
    self.__pivot = pivot
    self.__mesos = mesos
    self.__marathon = marathon
    self.__chronos = chronos

  @property
  def pivot(self):
    return self.__pivot

  @property
  def mesos(self):
    return self.__mesos

  @property
  def marathon(self):
    return self.__marathon

  @property
  def chronos(self):
    return self.__chronos

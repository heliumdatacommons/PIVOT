import swagger


@swagger.model
class Scheduler:

  def __init__(self, name, config={}):
    self.__name = name
    self.__config = dict(config)

  @property
  @swagger.property
  def name(self):
    """
    Scheduler module name
    ---
    type: str
    example: schedule.local.DefaultApplianceScheduler

    """
    return self.__name

  @property
  @swagger.property
  def config(self):
    """
    Scheduler configurations
    ---
    type: dict
    example:
      cfg_key: cfg_val

    """
    return dict(self.__config)

  def to_render(self):
    return dict(name=self.name, config=self.config)

  def to_save(self):
    return dict(name=self.name, config=self.config)


class SchedulePlan:

  def __init__(self, done=False, containers=[], volumes=[]):
    self.__done = done
    self.__containers = list(containers)
    self.__volumes = list(volumes)

  @property
  def done(self):
    return self.__done

  @property
  def containers(self):
    return list(self.__containers)

  @property
  def volumes(self):
    return list(self.__volumes)

  @done.setter
  def done(self, done):
    self.__done = done

  def add_containers(self, contrs):
    self.__containers += list(contrs)

  def add_volumes(self, vols):
    self.__volumes += list(vols)



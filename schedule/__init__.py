import swagger

from locality import Placement


@swagger.model
class Scheduler:

  @classmethod
  def parse(cls, data, from_user=True):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse scheduler data format: %s"%type(data)
    return 200, Scheduler(**data), None

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
    import container, volume
    assert all([isinstance(c, container.Container) for c in containers])
    assert all([isinstance(v, volume.PersistentVolume) for v in volumes])
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


@swagger.model
class ScheduleHints:

  def __init__(self, placement=None, *args, **kwargs):
    if isinstance(placement, dict):
      self.__placement = Placement(**placement)
    elif isinstance(placement, Placement):
      self.__placement = placement
    else:
      self.__placement = Placement()

  @property
  @swagger.property
  def placement(self):
    """
    Placement hints of the container
    ---
    type: Placement

    """
    return self.__placement

  @placement.setter
  def placement(self, placement):
    assert isinstance(placement, Placement)
    self.__placement = placement

  def to_render(self):
    return dict(placement=self.placement.to_render())

  def to_save(self):
    return dict(placement=self.placement.to_save())


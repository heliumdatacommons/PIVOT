import swagger

import locality
import schedule.task


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


@swagger.model
class ScheduleHints:

  def __init__(self, placement=None, *args, **kwargs):
    if isinstance(placement, dict):
      self.__placement = locality.Placement(**placement)
    elif isinstance(placement, locality.Placement):
      self.__placement = placement
    else:
      self.__placement = locality.Placement()

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
    assert isinstance(placement, locality.Placement)
    self.__placement = placement

  def to_render(self):
    return dict(placement=self.placement.to_render())

  def to_save(self):
    return dict(placement=self.placement.to_save())


class SchedulePlan:

  def __init__(self, tasks=[], volumes=[]):
    import volume
    assert all([isinstance(c, schedule.task.Task) for c in tasks])
    assert all([isinstance(v, volume.PersistentVolume) for v in volumes])
    self.__tasks = list(tasks)
    self.__volumes = list(volumes)

  @property
  def tasks(self):
    return list(self.__tasks)

  @property
  def volumes(self):
    return list(self.__volumes)

  def add_tasks(self, *tasks):
    self.__tasks += tasks

  def add_volumes(self, *vols):
    self.__volumes += vols
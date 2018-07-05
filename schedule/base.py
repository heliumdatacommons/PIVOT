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

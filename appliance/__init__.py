import swagger

from container import Container, get_short_ids
from schedule import Scheduler


@swagger.model
class Appliance:
  """
  PIVOT appliance

  """

  REQUIRED = frozenset(['id', 'containers'])
  ID_PATTERN = r'[a-zA-Z0-9-]+'

  @classmethod
  def parse(cls, data):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse appliance request format: %s"%type(data)
    missing = Appliance.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of appliance: %s"%missing
    containers, contr_ids = [], set()
    for c in data['containers']:
      if c['id'] in contr_ids:
        return 400, None, "Duplicate container id: %s"%c['id']
      status, contr, err = Container.parse(dict(**c, appliance=data['id']))
      if err:
        return status, None, err
      containers.append(contr)
      contr_ids.add(contr.id)
    addresses = set()
    for c in containers:
      if c.cmd:
        addresses.update(get_short_ids(c.cmd))
      for arg in c.args:
        addresses.update(get_short_ids(arg))
      for v in c.env.values():
        addresses.update(get_short_ids(v))
    undefined = list(addresses - contr_ids)
    if undefined:
      return 400, None, "Undefined container(s): %s"%undefined
    app = Appliance(containers=containers,
                    **{k: v for k, v in data.items() if k not in ('containers', )})
    return 200, app, None

  def __init__(self, id, containers=[], volumes=[],
               scheduler=Scheduler(name='schedule.local.DefaultApplianceScheduler'),
               **kwargs):
    self.__id = id
    self.__containers = list(containers)
    self.__volumes = list(volumes)
    self.__scheduler = scheduler if isinstance(scheduler, Scheduler) else Scheduler(**scheduler)

  @property
  @swagger.property
  def id(self):
    """
    Appliance ID

    ---
    type: str
    required: true
    example: test_app

    """
    return self.__id

  @property
  @swagger.property
  def containers(self):
    """
    Containers in the appliance

    ---
    type: list
    items: Container
    required: true

    """
    return self.__containers

  @property
  @swagger.property
  def volumes(self):
    """
    Persistent volumes shared among containers in the appliance

    ---
    type: list
    items: Volume
    required: true

    """
    return self.__volumes

  @property
  @swagger.property
  def scheduler(self):
    """
    Appliance-level scheduler for the appliance

    ---
    type: Scheduler

    """
    return self.__scheduler

  @containers.setter
  def containers(self, contrs):
    self.__containers = list(contrs)

  def to_render(self):
    return dict(id=self.id,
                scheduler=self.scheduler.to_render(),
                containers=[c.to_render() for c in self.containers])

  def to_save(self):
    return dict(id=self.id, scheduler=self.scheduler.to_save())

  def __str__(self):
    return self.id







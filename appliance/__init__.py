import swagger

from container import Container, get_short_ids
from volume import DataPersistence
from schedule import Scheduler


@swagger.model
class Appliance:
  """
  PIVOT appliance

  """

  REQUIRED = frozenset(['id', 'containers'])
  ID_PATTERN = r'[a-zA-Z0-9-]+'

  @classmethod
  def parse(cls, data, from_user=True):

    def validate_dependencies(contrs):
      contrs = {c.id: c for c in contrs}
      parents = {}
      for c in contrs.values():
        nonexist = list(filter(lambda c: c not in contrs, c.dependencies))
        if nonexist:
          return 422, None, "Dependencies '%s' do not exist in this appliance" % nonexist
      parents.setdefault(c.id, set()).update(c.dependencies)
      for c, p in parents.items():
        cycles = ['%s<->%s' % (c, pp)
                  for pp in filter(lambda x: c in parents.get(x, set()), p)]
        if cycles:
          return 422, None, "Cycle(s) found: %s" % cycles
      return 200, 'Dependencies are valid', None

    if not isinstance(data, dict):
      return 422, None, "Failed to parse appliance request format: %s"%type(data)
    missing = Appliance.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of appliance: %s"%missing
    fields = {}
    # instantiate container objects
    containers, contr_ids = [], set()
    for c in data.pop('containers', []):
      if c['id'] in contr_ids:
        return 400, None, "Duplicate container id: %s"%c['id']
      if from_user:
        for unwanted_f in ('appliance', ):
          c.pop(unwanted_f, None)
      status, contr, err = Container.parse(dict(**c, appliance=data['id']), from_user)
      if status != 200:
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
    status, msg, err = validate_dependencies(containers)
    if status != 200:
      return status, None, err
    fields.update(containers=containers)

    # instantiate data persistence object
    if 'data_persistence' in data:
      status, dp, err = DataPersistence.parse(dict(**data.pop('data_persistence', {}),
                                                   appliance=data['id']), from_user)
      if status != 200:
        return status, None, err
      fields.update(data_persistence=dp)

    # instantiate scheduler object
    if 'scheduler' in data:
      status, scheduler, err = Scheduler.parse(data.pop('scheduler', {}), from_user)
      if status != 200:
        return status, None, err
      fields.update(scheduler=scheduler)

    return 200, Appliance(**fields, **data), None

  def __init__(self, id, containers=[], data_persistence=None,
               scheduler=Scheduler(name='schedule.local.DefaultApplianceScheduler'),
               *args, **kwargs):
    self.__id = id
    self.__containers = list(containers)
    self.__data_persistence = data_persistence \
      if not data_persistence or isinstance(data_persistence, DataPersistence) \
      else DataPersistence(**data_persistence)
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
  def data_persistence(self):
    """
    Data persistence abstraction

    ---
    type: DataPersistence

    """
    return self.__data_persistence

  @property
  def volumes(self):
    return self.__data_persistence.volumes if self.__data_persistence else []

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
                containers=[c.to_render() for c in self.containers],
                data_persistence=self.data_persistence and self.data_persistence.to_render())

  def to_save(self):
    return dict(id=self.id, scheduler=self.scheduler.to_save(),
                data_persistence=self.data_persistence and self.data_persistence.to_save())

  def __hash__(self):
    return hash(self.id)

  def __eq__(self, other):
    return isinstance(other, Appliance) and self.id == other.id

  def __str__(self):
    return self.id







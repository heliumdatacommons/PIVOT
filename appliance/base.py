import swagger

from container.base import Container, get_short_ids


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
        return 400, None, "Duplicate container id: %s"%c.id
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
    app = Appliance(data['id'], containers)
    return 200, app, None

  def __init__(self, id, containers=[], **kwargs):
    self.__id = id
    self.__containers = list(containers)

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

  @containers.setter
  def containers(self, contrs):
    self.__containers = list(contrs)

  def to_render(self):
    return dict(id=self.id,
                containers=[c.to_render() for c in self.containers])

  def to_save(self):
    return dict(id=self.id)

  def __str__(self):
    return self.id







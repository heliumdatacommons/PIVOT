import swagger


@swagger.model
class Volume:
  """
  Distributed persistent volume shared among containers in an appliance

  """

  @classmethod
  def parse(cls, data):
    return 200, Volume(**data), None

  def __init__(self, id, appliance, *args, **kwargs):
    self.__id = id
    self.__appliance = appliance

  @property
  @swagger.property
  def id(self):
    """
    Volume name
    ---
    type: str
    required: true
    example: alpha

    """
    return self.__id

  @property
  @swagger.property
  def appliance(self):
    """
    The appliance in which the volume is shared
    ---
    type: str
    example: test-app
    read_only: true

    """
    return self.__appliance

  def to_render(self):
    return dict(id=self.id, appliance=self.appliance)

  def to_save(self):
    return self.to_render()

  def to_request(self):
    return dict(name='%s-%s'%(self.appliance, self.id))

  def __hash__(self):
    return hash((self.id, self.appliance))

  def __eq__(self, other):
    return self.__class__ == other.__class__ \
            and self.id == other.id \
            and self.appliance == other.appliance

  def __str__(self):
    return '%s-%s'%(self.appliance, self.id)



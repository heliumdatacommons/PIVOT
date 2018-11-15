import swagger
import appliance

from enum import Enum


@swagger.enum
class PersistentVolumeType(Enum):
  """
  Volume type

  """
  CEPHFS = 'cephfs', 'heliumdatacommons/cephfs'

  def __new__(cls, value, driver):
    obj = object.__new__(cls)
    obj._value_ = value
    obj.driver = driver
    return obj


@swagger.model
class DataPersistence:
  """
  Data persistence abstraction for an appliance

  """
  REQUIRED = frozenset(['volumes'])

  @classmethod
  def parse(cls, data, appliance):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse data_persistence request format: %s"%type(data)
    missing = DataPersistence.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of data_persistence: %s"%missing
    volume_type = data.pop('volume_type', None) or PersistentVolumeType.CEPHFS
    volume_type = volume_type if isinstance(volume_type, PersistentVolumeType) \
      else PersistentVolumeType(volume_type)
    volumes, vol_ids = [], set()
    for v in data.pop('volumes', []):
      if v['id'] in vol_ids:
        return 400, None, "Duplicate volume id: %s"%v['id']
      status, contr, err = PersistentVolume.parse(dict(**v, type=volume_type, appliance=appliance))
      if status != 200:
        return status, None, err
      volumes.append(contr)
      vol_ids.add(contr.id)
    return 200, DataPersistence(volumes=volumes, volume_type=volume_type, **data), None

  def __init__(self, volume_type=PersistentVolumeType.CEPHFS, volumes=[]):
    self.__volume_type = volume_type if isinstance(volume_type, PersistentVolumeType) \
      else PersistentVolumeType(volume_type)
    self.__volumes = list(volumes)

  @property
  @swagger.property
  def volume_type(self):
    """
    Volume type for data persistence
    ---
    type: volume.VolumeType
    example: cephfs

    """
    return self.__volume_type

  @property
  def volumes(self):
    """
    Volumes for data persistence in the appliance
    ---
    type: list
    items: Volume
    required: true

    """
    return list(self.__volumes)

  @volumes.setter
  def volumes(self, volumes):
    self.__volumes = list(volumes)

  def to_render(self):
    return dict(volume_type=self.volume_type.value, volumes=[v.to_render() for v in self.volumes])

  def to_save(self):
    return dict(volume_type=self.volume_type.value)


@swagger.model
class PersistentVolume:
  """
  Distributed persistent volume shared among containers in an appliance

  """
  REQUIRED = frozenset(['id'])

  @classmethod
  def parse(cls, data):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse volume request format: %s"%type(data)
    missing = PersistentVolume.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of persistence volume: %s"%missing
    return 200, PersistentVolume(**data), None

  def __init__(self, id, appliance, type, is_instantiated=False, *args, **kwargs):
    self.__id = id
    self.__appliance = appliance
    self.__type = type if isinstance(type, PersistentVolumeType) else PersistentVolumeType(type)
    self.__is_instantiated = is_instantiated

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

  @property
  @swagger.property
  def is_instantiated(self):
    """
    Whether the volume is instantiated
    ---
    type: bool
    read_only: true

    """
    return self.__is_instantiated

  @property
  def type(self):
    """
    Volume type
    ---
    type: Volume
    example: cephfs

    """
    return self.__type

  @appliance.setter
  def appliance(self, app):
    assert isinstance(app, str) or isinstance(app, appliance.Appliance)
    self.__appliance = app

  @type.setter
  def type(self, type):
    self.__type = type

  def set_instantiated(self):
    self.__is_instantiated = True

  def to_render(self):
    return dict(id=self.id,
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id,
                type=self.type.value, is_instantiated=self.is_instantiated)

  def to_save(self):
    return dict(id=self.id,
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id,
                type=self.type.value, is_instantiated=self.is_instantiated)

  def to_request(self):
    return dict(name='%s-%s'%(self.appliance, self.id))

  def __hash__(self):
    return hash((self.id, self.appliance))

  def __eq__(self, other):
    return isinstance(other, PersistentVolume) \
            and self.id == other.id \
            and ((isinstance(self.appliance, appliance.Appliance)
                 and isinstance(other.appliance, appliance.Appliance)
                 and self.appliance == other.appliance)
              or (isinstance(self.appliance, str)
                 and isinstance(other.appliance, str)
                 and self.appliance == other.appliance))

  def __str__(self):
    return '%s-%s'%(self.appliance, self.id)



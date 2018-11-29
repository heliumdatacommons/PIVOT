import swagger
import appliance
import schedule

from enum import Enum
from locality import Placement


@swagger.enum
class PersistentVolumeType(Enum):
  """
  Persistent volume type

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
  def parse(cls, data, from_user=True):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse data_persistence request format: %s"%type(data)
    missing = DataPersistence.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of data_persistence: %s"%missing
    volume_type = data.pop('volume_type', None) or PersistentVolumeType.CEPHFS
    volume_type = volume_type if isinstance(volume_type, PersistentVolumeType) \
      else PersistentVolumeType(volume_type)
    volumes, vol_ids, appliance = [], set(), data.pop('appliance', None)
    for v in data.pop('volumes', []):
      if v['id'] in vol_ids:
        return 400, None, "Duplicate volume id: %s"%v['id']
      status, contr, err = PersistentVolume.parse(dict(**v, appliance=appliance, type=volume_type),
                                                  from_user)
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
class VolumeScheduleHints(schedule.ScheduleHints):

  @classmethod
  def parse(self, data, from_user=True):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse container data format: %s" % type(data)
    return 200, VolumeScheduleHints(**data), None

  def __init__(self, *args, **kwargs):
    super(VolumeScheduleHints, self).__init__(*args, **kwargs)


@swagger.enum
class VolumeScope(Enum):
  """
  Persistent volume scope

  """

  GLOBAL = 'GLOBAL'
  LOCAL = 'LOCAL'


@swagger.model
class PersistentVolume:
  """
  Distributed persistent volume shared among containers in an appliance

  """

  REQUIRED = frozenset(['id'])
  ID_PATTERN = r'[a-zA-Z0-9-]+'

  @classmethod
  def parse(cls, data, from_user=True):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse volume request format: %s"%type(data)
    missing = PersistentVolume.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of persistence volume: %s"%missing
    if from_user:
      for f in ('deployment', ):
        data.pop('deployment', None)
      sched_hints = data.pop('schedule_hints', None)
      if sched_hints:
        status, sched_hints, err = VolumeScheduleHints.parse(sched_hints, from_user)
        if status != 200:
          return status, None, err
        data['user_schedule_hints'] = sched_hints
    else:
      user_sched_hints = data.get('user_schedule_hints')
      sys_sched_hints = data.get('sys_schedule_hints')
      if user_sched_hints:
        _, data['user_schedule_hints'], _ = VolumeScheduleHints.parse(user_sched_hints, from_user)
      if sys_sched_hints:
        _, data['sys_schedule_hints'], _ = VolumeScheduleHints.parse(sys_sched_hints, from_user)
    return 200, PersistentVolume(**data), None

  def __init__(self, id, appliance, type, is_instantiated=False,
               scope=VolumeScope.LOCAL, user_schedule_hints=None, sys_schedule_hints=None,
               deployment=None, *args, **kwargs):
    self.__id = id
    self.__appliance = appliance
    self.__scope = scope if isinstance(scope, VolumeScope) else VolumeScope(scope.upper())
    self.__type = type if isinstance(type, PersistentVolumeType) else PersistentVolumeType(type)
    self.__is_instantiated = is_instantiated

    if isinstance(user_schedule_hints, dict):
      self.__user_schedule_hints = VolumeScheduleHints(**user_schedule_hints)
    elif isinstance(user_schedule_hints, VolumeScheduleHints):
      self.__user_schedule_hints = user_schedule_hints
    else:
      self.__user_schedule_hints = VolumeScheduleHints()

    if isinstance(sys_schedule_hints, dict):
      self.__sys_schedule_hints = VolumeScheduleHints(**sys_schedule_hints)
    elif isinstance(sys_schedule_hints, VolumeScheduleHints):
      self.__sys_schedule_hints = sys_schedule_hints
    else:
      self.__sys_schedule_hints = VolumeScheduleHints()

    if isinstance(deployment, dict):
      self.__deployment = VolumeDeployment(**deployment)
    elif isinstance(deployment, VolumeDeployment):
      self.__deployment = deployment
    else:
      self.__deployment = VolumeDeployment()


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
  @swagger.property
  def scope(self):
    """
    Persistent volume scope
    ---
    type: PersistentVolumeScope

    """
    return self.__scope

  @property
  @swagger.property
  def type(self):
    """
    Volume type
    ---
    type: Volume
    example: cephfs

    """
    return self.__type

  @property
  @swagger.property
  def user_schedule_hints(self):
    """
    User-specified volume schedule hints
    ---
    type: VolumeScheduleHints

    """
    return self.__user_schedule_hints

  @property
  @swagger.property
  def sys_schedule_hints(self):
    """
    System volume schedule hints
    ---
    type: VolumeScheduleHints
    read_only: true

    """
    return self.__sys_schedule_hints

  @property
  @swagger.property
  def deployment(self):
    """
    Volume deployment
    ---
    type: VolumeDeployment
    read_only: true

    """
    return self.__deployment

  @appliance.setter
  def appliance(self, app):
    assert isinstance(app, str) or isinstance(app, appliance.Appliance)
    self.__appliance = app

  @type.setter
  def type(self, type):
    self.__type = type

  @sys_schedule_hints.setter
  def sys_schedule_hints(self, hints):
    assert isinstance(hints, VolumeScheduleHints)
    self.__sys_schedule_hints = hints

  @deployment.setter
  def deployment(self, deployment):
    self.__deployment = deployment

  def set_instantiated(self):
    self.__is_instantiated = True

  def unset_instantiated(self):
    self.__is_instantiated = False

  def to_render(self):
    return dict(id=self.id,
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id,
                type=self.type.value, is_instantiated=self.is_instantiated,
                scope=self.scope.value,
                user_schedule_hints=self.user_schedule_hints.to_render(),
                sys_schedule_hints=self.sys_schedule_hints.to_render(),
                deployment=self.deployment.to_render())

  def to_save(self):
    return dict(id=self.id,
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id,
                type=self.type.value, is_instantiated=self.is_instantiated,
                scope=self.scope.value,
                user_schedule_hints=self.user_schedule_hints.to_save(),
                sys_schedule_hints=self.sys_schedule_hints.to_save(),
                deployment=self.deployment.to_render())

  def to_request(self):
    req = dict(name=('%s-%s'%(self.appliance, self.id)
                     if self.scope == VolumeScope.LOCAL else str(self.id)))
    sched_hints = self.sys_schedule_hints.placement
    if sched_hints.host:
      req['placement'] = dict(type='host', value=sched_hints.host)
    elif sched_hints.zone:
      req['placement'] = dict(type='zone', value=sched_hints.zone)
    elif sched_hints.region:
      req['placement'] = dict(type='region', value=sched_hints.region)
    elif sched_hints.cloud:
      req['placement'] = dict(type='cloud', value=sched_hints.cloud)
    return req

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


@swagger.model
class VolumeDeployment:

  def __init__(self, placement=None):
    if isinstance(placement, dict):
      self.__placement = Placement(**placement)
    elif isinstance(placement, Placement):
      self.__placement = placement
    else:
      self.__placement = Placement()

  @property
  @swagger.property
  def placement(self):
    return self.__placement

  def to_render(self):
    return dict(placement=self.placement.to_render())

  def to_save(self):
    return dict(placement=self.placement.to_render())
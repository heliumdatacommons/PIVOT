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


@swagger.enum
class PersistentVolumeState(Enum):
  """
  Persistent volume state
  """
  CREATED = 'created'
  INACTIVE = 'inactive'
  ACTIVE = 'active'


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
  @swagger.property
  def volumes(self):
    """
    Persistent volumes used by the appliance
    ---
    type: list
    items: PersistentVolume
    required: true

    """
    return list(self.__volumes)

  @property
  def global_volumes(self):
    """
    Global persistent volumes used by the appliance
    ---
    type: list
    item: GlobalPersistentVolume

    """
    return [v for v in self.__volumes if isinstance(v, GlobalPersistentVolume)]

  @property
  def local_volumes(self):
    """
    Local persistent volumes bound to the appliance
    ---
    type: list
    item: LocalPersistentVolume

    """
    return [v for v in self.__volumes if isinstance(v, LocalPersistentVolume)]

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
    scope = data.get('scope', 'local')
    try:
      scope = VolumeScope(scope and scope.upper())
    except ValueError:
      return 400, None, "Invalid volume scope: %s"%data.get('scope')
    if from_user:
      for f in ('deployment', ):
        data.pop(f, None)
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
    vol = None
    if scope == VolumeScope.LOCAL:
      vol = LocalPersistentVolume(**data)
    elif scope == VolumeScope.GLOBAL:
      vol = GlobalPersistentVolume(**data)
    else:
      return 400, None, "Unrecognized volume scope: %s"%vol.value
    return 200, vol, None

  def __init__(self, id, type, state=PersistentVolumeState.CREATED,
               scope=VolumeScope.LOCAL, user_schedule_hints=None, sys_schedule_hints=None,
               deployment=None, *args, **kwargs):
    self.__id = str(id)
    self.__scope = VolumeScope(scope.upper()) if isinstance(scope, str) else scope
    self.__type = PersistentVolumeType(type.lower()) if isinstance(type, str) else type
    self.__state = PersistentVolumeState(state.lower()) if isinstance(state, str) else state

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
  def type(self):
    """
    Volume type
    ---
    type: PersistentVolumeType
    example: cephfs

    """
    return self.__type

  @property
  def state(self):
    """
    Volume state
    ---
    type: PersistentVolumeState

    """
    return self.__state

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

  @property
  def is_active(self):
    return self.__state == PersistentVolumeState.ACTIVE

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

  def set_active(self):
    self.__state = PersistentVolumeState.ACTIVE

  def set_inactive(self):
    self.__state = PersistentVolumeState.INACTIVE

  def to_render(self):
    return dict(id=self.id,
                type=self.type.value,
                state=self.state.value,
                scope=self.scope.value,
                user_schedule_hints=self.user_schedule_hints.to_render(),
                sys_schedule_hints=self.sys_schedule_hints.to_render(),
                deployment=self.deployment.to_render())

  def to_save(self):
    return dict(id=self.id,
                type=self.type.value,
                state=self.state.value,
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
    return '%s-%s'%(self.appliance, self.id) if self.scope == VolumeScope.LOCAL else str(self.id)


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
  

@swagger.model
class GlobalPersistentVolume(PersistentVolume):
  
  def __init__(self, used_by=[], *args, **kwargs):
    kwargs.update(scope=VolumeScope.GLOBAL)
    super(GlobalPersistentVolume, self).__init__(*args, **kwargs)
    self.__used_by = set(used_by)
    
  @property
  @swagger.property
  def used_by(self):
    """
    Appliances that use the volume
    ---
    type: list
    items: str

    """
    return list(self.__used_by)

  def subscribe(self, app_id):
    self.__used_by.add(app_id)

  def unsubscribe(self, app_id):
    self.__used_by.remove(app_id)

  def to_save(self):
    return dict(**super(GlobalPersistentVolume, self).to_save(), used_by=self.used_by)
  

@swagger.model
class LocalPersistentVolume(PersistentVolume):
  
  def __init__(self, appliance, *args, **kwargs):
    kwargs.update(scope=VolumeScope.LOCAL)
    super(LocalPersistentVolume, self).__init__(*args, **kwargs)
    self.__appliance = appliance

  @property
  @swagger.property
  def appliance(self):
    """
    The appliance which the persistent volume is bound to
    ---
    type: str

    """
    return self.__appliance

  @appliance.setter
  def appliance(self, app):
    assert isinstance(app, str) or isinstance(app, appliance.Appliance)
    self.__appliance = app

  def to_render(self):
    return dict(**super(LocalPersistentVolume, self).to_render(),
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id)

  def to_save(self):
    return dict(**super(LocalPersistentVolume, self).to_save(),
                appliance=self.appliance if isinstance(self.appliance, str) else self.appliance.id)

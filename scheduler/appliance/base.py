from container.base import ContainerState, ContainerType
from container.manager import ContainerManager
from commons import Loggable


class ApplianceDAG:

  @classmethod
  def construct_dag(cls, app):
    dag = ApplianceDAG(app)
    status, msg, err = dag.construct()
    if status != 200:
      return status, msg, err
    return status, dag, None

  def __init__(self, app, parent_map={}, child_map={}, *args, **kwargs):
    self.__app = app
    self.__containers = {c.id: c for c in app.containers}
    self.__parent_map = {k: set(v) for k, v in parent_map.items()}
    self.__child_map = {k: set(v) for k, v in child_map.items()}

  @property
  def appliance(self):
    return self.__app.id

  @property
  def parent_map(self):
    return {k: list(v) for k, v in self.__parent_map.items()}

  @property
  def child_map(self):
    return {k: list(v) for k, v in self.__child_map.items()}

  @property
  def is_empty(self):
    return len(self.__parent_map) == 0

  def get_free_containers(self):
    return [self.__containers[k] for k, v in self.__parent_map.items() if not v]

  def update_container(self, contr):
    self.__containers[contr.id] = contr

  def remove_container(self, contr_id):
    for child in self.__child_map.pop(contr_id, set()):
      self.__parent_map.setdefault(child, set()).remove(contr_id)
    self.__parent_map.pop(contr_id, None)

  def to_save(self):
    return dict(appliance=self.appliance,
                parent_map=self.parent_map, child_map=self.child_map)

  def construct(self):
    parent_map, child_map = self.__parent_map, self.__child_map
    if not parent_map or not child_map:
      # check dependency validity
      for c in self.__containers.values():
        nonexist_deps = list(filter(lambda c: c not in self.__containers, c.dependencies))
        if nonexist_deps:
          return 422, None, "Dependencies '%s' do not exist in this appliance"%nonexist_deps
        parent_map.setdefault(c.id, set()).update(c.dependencies)
        for d in c.dependencies:
          child_map.setdefault(d, set()).add(c.id)
      # check cycles
      for c, parents in parent_map.items():
        cyclic_deps = ['%s<->%s'%(c, p)
                       for p in filter(lambda p: c in parent_map.setdefault(p, set()),
                                       parents)]
        if cyclic_deps:
          return 422, None, "Cycle(s) found: %s"%cyclic_deps
    self.__parent_map, self.__child_map = parent_map, child_map
    return 200, "DAG constructed successfully", None


class SchedulePlan(Loggable):

  def __init__(self, id, contrs):
    self.__id = id
    self.__contr_mgr = ContainerManager()
    self.__waiting = {c: 0 for c in contrs}
    self.__provisioned = set()
    self.__done = set()
    self.__failed = set()
    self.__is_stopped = False

  @property
  def id(self):
    return self.__id

  @property
  def is_stopped(self):
    return self.__is_stopped

  @property
  def is_finished(self):
    return not self.__waiting and not self.__provisioned and self.__done

  @property
  def waiting(self):
    return dict(self.__waiting)

  @property
  def provisioned(self):
    return set(self.__provisioned)

  @property
  def done(self):
    return set(self.__done)

  @property
  def failed(self):
    return set(self.__failed)

  def stop(self):
    self.__is_stopped = True

  async def execute(self):
    for c, n_retry in dict(self.__waiting).items():
      if c.state != ContainerState.SUBMITTED:
        continue
      self.logger.info('Launch container: %s'%c)
      status, _, err = await self.__contr_mgr.provision_container(c)
      if status in (200, 409):
        if self.__waiting.pop(c, None) is not None:
          self.__provisioned.add(c)
      else:
        self.logger.info(status)
        self.logger.error("Failed to launch container '%s'"%c)
        self.logger.error(err)
        if n_retry < 3:
          self.__waiting[c] += 1
        else:
          self.__failed.add(c)

  async def update(self):
    for c in list(self.__provisioned):
      status, c, err = await self.__contr_mgr.get_container(c.appliance, c.id)
      if status != 200:
        self.logger.error(err)
        continue
      if (c.type == ContainerType.SERVICE and c.state == ContainerState.RUNNING) \
          or (c.type == ContainerType.JOB and c.state == ContainerState.SUCCESS):
        self.__provisioned.remove(c)
        self.__done.add(c)

from abc import ABCMeta, abstractmethod

from container.base import ContainerState, ContainerType
from container.manager import ContainerManager

from commons import AutonomousMonitor, Loggable


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
    for c in self.__provisioned:
      status, c, err = await self.__contr_mgr.get_container(c.appliance, c.id)
      if status != 200:
        self.logger.error(err)
        continue
      if (c.type == ContainerType.SERVICE and c.state == ContainerState.RUNNING) \
          or (c.type == ContainerType.JOB and c.state == ContainerState.SUCCESS):
        self.__provisioned.remove(c)
        self.__done.add(c)


class AbstractApplianceScheduler(AutonomousMonitor, metaclass=ABCMeta):

  def __init__(self, interval=3000):
    super(AbstractApplianceScheduler, self).__init__(interval)
    self.__plans = {}

  @property
  def plans(self):
    return dict(self.__plans)

  async def callback(self):
    self.__plans.update({p.id: p for p in await self.schedule(self.plans)
                         if p.id not in self.__plans})
    if self.is_running and not self.__plans:
      self.logger.info('No further schedule plan, stop scheduler')
      self.stop()
      return
    for pid, p in dict(self.__plans).items():
      if p.is_stopped or p.is_finished:
        self.logger.info("Plan '%s' is finished, removing it"%pid)
        self.__plans.pop(pid, None)
        continue
      if p.waiting:
        self.logger.info("Waiting container(s): %s"%[c.id for c in p.waiting.keys()])
        self.logger.info("Execute the plan")
        await p.execute()
      await p.update()

  @abstractmethod
  async def schedule(self, plans):
    raise NotImplemented


class DefaultApplianceScheduler(AbstractApplianceScheduler):

  def __init__(self, app):
    super(DefaultApplianceScheduler, self).__init__()
    from appliance.manager import ApplianceManager
    self.__app_mgr = ApplianceManager()
    self.__app = app

  async def schedule(self, plans):
    await self._ensure_appliance_exist()
    for p in plans.values():
      if p.failed:
        p.stop()
        continue
      for c in p.provisioned:
        self.__app.dag.update_container(c)
      for c in p.done:
        self.__app.dag.remove_container(c.id)
    self.logger.debug("Update the DAG in the database")
    status, msg, err = await self.__app_mgr.save_appliance(self.__app, False)
    if err:
      self.logger.error(err)
    contrs = self.__app.dag.get_free_containers()
    self.logger.info('Free containers: %s'%[c.id for c in contrs])
    new_plans = [SchedulePlan(c.id, [c]) for c in contrs if c.id not in plans]
    if new_plans:
      self.logger.info('New plans: %s'%[p.id for p in new_plans])
    return new_plans

  async def _ensure_appliance_exist(self):
    status, _, _ = await self.__app_mgr.get_appliance(self.__app.id)
    if status == 404:
      self.logger.info("Appliance '%s' is already deleted, stop scheduling"%self.__app.id)
      self.stop()

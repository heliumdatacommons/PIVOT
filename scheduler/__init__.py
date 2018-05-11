from abc import ABCMeta, abstractmethod

from scheduler.base import ApplianceDAG, SchedulePlan
from scheduler.manager import SchedulerManager

from commons import AutonomousMonitor


class AbstractApplianceScheduler(AutonomousMonitor, metaclass=ABCMeta):

  def __init__(self, interval=3000):
    super(AbstractApplianceScheduler, self).__init__(interval)
    self.__sched_mgr = SchedulerManager()
    self.__plans = {}

  @property
  def sched_mgr(self):
    return self.__sched_mgr

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
  async def initialize(self): pass

  @abstractmethod
  async def schedule(self, plans): pass


class DefaultApplianceScheduler(AbstractApplianceScheduler):

  def __init__(self, app):
    super(DefaultApplianceScheduler, self).__init__()
    self.__app = app
    self.__dag = None

  @property
  def dag(self):
    return self.__dag

  async def initialize(self):
    self.__dag = ApplianceDAG(self.__app)
    status, msg, err = self.__dag.construct()
    await self.sched_mgr.update_appliance_dag(self.__dag, True)
    return status, msg, err

  async def schedule(self, plans):
    assert self.dag is not None
    await self.ensure_appliance_exist()
    for p in plans.values():
      if p.failed:
        self.logger.info("Plan '%s' Failed"%p.id)
        p.stop()
        continue
      for c in p.provisioned:
        self.dag.update_container(c)
      for c in p.done:
        self.dag.remove_container(c.id)
    self.logger.info("Update the DAG in the database")
    status, msg, err = await self.sched_mgr.update_appliance_dag(self.dag)
    if err:
      self.logger.error(err)
    contrs = self.dag.get_free_containers()
    self.logger.info('Free containers: %s'%[c.id for c in contrs])
    new_plans = [SchedulePlan(c.id, [c]) for c in contrs if c.id not in plans]
    if new_plans:
      self.logger.info('New plans: %s'%[p.id for p in new_plans])
    return new_plans

  async def ensure_appliance_exist(self):
    status, _, _ = await self.sched_mgr.get_appliance_dag(self.__app.id)
    if status == 404:
      self.logger.info("Appliance '%s' is already deleted, stop scheduling"%self.__app.id)
      self.stop()



import appliance

from abc import ABCMeta

from schedule import GlobalScheduleExecutor, SchedulePlan
from commons import AutonomousMonitor, Loggable
from config import get_global_scheduler


class ApplianceScheduleExecutor(AutonomousMonitor):

  def __init__(self, app_id, scheduler, interval=3000):
    super(ApplianceScheduleExecutor, self).__init__(interval)
    self.__app_id = app_id
    self.__app_mgr = appliance.manager.ApplianceManager()
    self.__local_sched = scheduler
    self.__global_sched_exec = GlobalScheduleExecutor(get_global_scheduler())

  async def callback(self):
    # get appliance
    status, app, err = await self.__app_mgr.get_appliance(self.__app_id)
    if not app:
      if status == 404:
        self.logger.info('Appliance %s no longer exists'%self.__app_id)
      else:
        self.logger.error(err)
      self.stop()
      return
    # get cluster info
    agents = await self.__global_sched_exec.get_agents()
    # contact the scheduler for new schedule
    sched = await self.__local_sched.schedule(app, agents)
    self.logger.debug('Containers to be scheduled: %s'%[c.id for c in sched.containers])
    # if the scheduling is done
    if sched.done:
      self.logger.info('Scheduling is done for appliance %s'%self.__app_id)
      self.stop()
      return
    # execute the new schedule
    await self._execute(sched)

  async def _execute(self, sched):
    for c in sched.containers:
      await self.__global_sched_exec.submit(c)
      self.logger.info('Container %s is being scheduled'%c.id)


class ApplianceScheduler(Loggable, metaclass=ABCMeta):

  async def schedule(self, app, agents):
    raise NotImplemented


from container.base import ContainerState, ContainerType


class DefaultApplianceScheduler(ApplianceScheduler):

  async def schedule(self, app, agents):
    sched = SchedulePlan()
    free_contrs = self.resolve_dependencies(app)
    self.logger.debug('Free containers: %s'%[c.id for c in free_contrs])
    if not free_contrs:
      sched.done = True
      return sched
    sched.add_containers([c for c in free_contrs if c.state in
                          (ContainerState.SUBMITTED, ContainerState.FAILED)])
    return sched

  def resolve_dependencies(self, app):
    contrs = {c.id: c for c in app.containers
              if (c.type == ContainerType.JOB and c.state != ContainerState.SUCCESS)
              or (c.type == ContainerType.SERVICE and c.state != ContainerState.RUNNING)}
    parents = {}
    for c in contrs.values():
      parents.setdefault(c.id, set()).update([d for d in c.dependencies if d in contrs])
    return [contrs[k] for k, v in parents.items() if not v]

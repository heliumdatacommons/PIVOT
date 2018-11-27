import appliance.manager

from abc import ABCMeta

from schedule import SchedulePlan
from schedule.universal import GlobalScheduleExecutor
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
        self.logger.info("Appliance '%s' no longer exists"%self.__app_id)
      else:
        self.logger.error(err)
      self.stop()
      return
    # get cluster info
    agents = await self.__global_sched_exec.get_agents()
    # contact the scheduler for new schedule
    sched = await self.__local_sched.schedule(app, list(agents))
    self.logger.debug('Containers to be scheduled: %s'%[c.id for c in sched.containers])
    # if the scheduling is done
    if sched.done:
      self.logger.info('Scheduling is done for appliance %s'%self.__app_id)
      self.stop()
      return
    # execute the new schedulex
    await self.__global_sched_exec.submit(sched)


class ApplianceScheduler(Loggable, metaclass=ABCMeta):

  def __init__(self, config={}):
    self.__config = dict(config)

  @property
  def config(self):
    return dict(self.__config)

  async def schedule(self, app, agents):
    """
    Caution: the parameters should not be overridden by schedulers that extend
    this class, otherwise inconsistency of appliance/cluster info will be caused.

    """
    raise NotImplemented


from container import ContainerState, ContainerType, ContainerVolumeType


class DefaultApplianceScheduler(ApplianceScheduler):

  def __init__(self, *args, **kwargs):
    super(DefaultApplianceScheduler, self).__init__(*args, **kwargs)

  async def schedule(self, app, agents):
    """

    :param app: appliance.Appliance
    :param agents: cluster.Agent
    :return: schedule.SchedulePlan

    """
    sched = SchedulePlan()
    free_contrs = self.resolve_dependencies(app)
    self.logger.info('Free containers: %s'%[c.id for c in free_contrs])
    if not free_contrs:
      sched.done = True
      return sched
    contrs_to_create = [c for c in free_contrs
                        if c.state in (ContainerState.SUBMITTED, ContainerState.FAILED)]
    for c in contrs_to_create:
      c.sys_schedule_hints = c.user_schedule_hints
    vols_declared = {v.id: v for v in app.volumes}
    vols_to_create = set([v.src for c in free_contrs for v in c.persistent_volumes
                          if v.type == ContainerVolumeType.PERSISTENT
                          and v.src in vols_declared
                          and not vols_declared[v.src].is_instantiated])
    for vid in vols_to_create:
      vols_declared[vid].sys_schedule_hints = vols_declared[vid].user_schedule_hints
    sched.add_containers(contrs_to_create)
    sched.add_volumes([vols_declared[vid] for vid in vols_to_create])
    return sched

  def resolve_dependencies(self, app):
    contrs = {c.id: c for c in app.containers
              if (c.type == ContainerType.JOB and c.state != ContainerState.SUCCESS)
              or (c.type == ContainerType.SERVICE and c.state != ContainerState.RUNNING)}
    parents = {}
    for c in contrs.values():
      parents.setdefault(c.id, set()).update([d for d in c.dependencies if d in contrs])
    return [contrs[k] for k, v in parents.items() if not v]

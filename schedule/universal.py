import container.manager
import cluster.manager
import volume.manager

from tornado.gen import multi

from schedule import SchedulePlan
from commons import AutonomousMonitor, Singleton, Loggable


class GlobalScheduler(Loggable, metaclass=Singleton):

  async def schedule(self, sched, agents):
    """

    :param sched: schedule.SchedulePlan
    :param agents: cluster.Agent
    :return: schedule.SchedulePlan
    """
    raise NotImplemented

  async def reschedule(self, contrs, agents):
    raise NotImplemented


class GlobalScheduleExecutor(Loggable, metaclass=Singleton):

  def __init__(self, scheduler, interval=30000):
    super(GlobalScheduleExecutor, self).__init__()
    self.__scheduler = scheduler
    self.__contr_mgr = container.manager.ContainerManager()
    self.__cluster_mgr = cluster.manager.ClusterManager()
    self.__vol_mgr = volume.manager.VolumeManager()
    self.__resched_runner = RescheduleRunner(scheduler, self, interval)

  def start_rescheduler(self):
    self.__resched_runner.start()

  async def submit(self, sched):
    """

    :param sched: schedule.SchedulePlan

    """
    assert isinstance(sched, SchedulePlan)

    agents = await self.get_agents()
    plan = await self.__scheduler.schedule(sched, list(agents))
    await multi([self.provision_volume(v) for v in plan.volumes])
    await multi([self.provision_container(c) for c in plan.containers])

  async def get_agents(self):
    return await self.__cluster_mgr.get_cluster(0)

  async def get_containers(self, **kwargs):
    status, contrs, err = await self.__contr_mgr.get_containers(**kwargs, full_blown=True)
    if status != 200:
      self.logger.error(err)
    return contrs

  async def provision_volume(self, vol):
    self.logger.info("Volume '%s' is being provisioned"%vol.id)
    status, _, err = await self.__vol_mgr.provision_volume(vol)
    if status != 200:
      self.logger.error(err)

  async def provision_container(self, contr):
    self.logger.info("Container '%s' is being provisioned"%contr.id)
    await self.__contr_mgr.save_container(contr)
    status, contr, err = await self.__contr_mgr.get_container(contr.appliance, contr.id,
                                                              full_blown=True)
    status, contr, err = await self.__contr_mgr.provision_container(contr)
    if err:
      self.logger.error(err)


class RescheduleRunner(AutonomousMonitor):

  def __init__(self, scheduler, executor, interval=30000):
    super(RescheduleRunner, self).__init__(interval)
    self.logger.info('Global scheduler: %s'%scheduler.__class__.__name__)
    self.__scheduler = scheduler
    self.__executor = executor

  async def callback(self):
    agents = await self.__executor.get_agents()
    contrs = await self.__executor.get_containers()
    if not contrs: return
    plan = await self.__scheduler.reschedule(contrs, agents)
    await multi([self.__executor.provision_container(c) for c in plan.containers])


class DefaultGlobalScheduler(GlobalScheduler):

  async def schedule(self, sched, agents):
    return sched

  async def reschedule(self, contrs, agents):
    return SchedulePlan()

import container.manager
import cluster.manager

from tornado.gen import multi

from schedule import SchedulePlan
from commons import AutonomousMonitor, Singleton, Loggable


class GlobalScheduler(Loggable, metaclass=Singleton):

  async def schedule(self, contr, agents):
    raise NotImplemented

  async def reschedule(self, contrs, agents):
    raise NotImplemented


class GlobalScheduleExecutor(Loggable, metaclass=Singleton):

  def __init__(self, scheduler, interval=30000):
    super(GlobalScheduleExecutor, self).__init__()
    self.__scheduler = scheduler
    self.__contr_mgr = container.manager.ContainerManager()
    self.__cluster_mgr = cluster.manager.ClusterManager()
    self.__resched_runner = RescheduleRunner(scheduler, self, interval)

  def start_rescheduler(self):
    self.__resched_runner.start()

  async def submit(self, contr):
    agents = await self.get_agents()
    plan = await self.__scheduler.schedule(contr, list(agents))
    await multi([self.provision_container(c) for c in plan.containers])

  async def get_agents(self):
    return await self.__cluster_mgr.get_cluster(0)

  async def get_containers(self, **kwargs):
    status, contrs, err = await self.__contr_mgr.get_containers(**kwargs)
    if err:
      self.logger.error(err)
    return contrs

  async def provision_container(self, contr):
    await self.__contr_mgr.save_container(contr)
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

  async def schedule(self, contr, agents):
    return SchedulePlan(containers=[contr])

  async def reschedule(self, contrs, agents):
    return SchedulePlan()

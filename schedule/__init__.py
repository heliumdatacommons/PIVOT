import container
import cluster

from commons import AutonomousMonitor, Singleton, Loggable


class SchedulePlan:

  def __init__(self, done=False, containers=[]):
    self.__done = done
    self.__containers = list(containers)

  @property
  def done(self):
    return self.__done

  @property
  def containers(self):
    return list(self.__containers)

  @done.setter
  def done(self, done):
    self.__done = done

  def add_containers(self, contrs):
    self.__containers += list(contrs)


class GlobalScheduler(Loggable, metaclass=Singleton):

  async def schedule(self, contr, agents):
    # generate a schedule plan
    # return the schedule plan
    pass


  async def reschedule(self, contrs, agents):
    # collect evidence for scheduling
    # generate a reschedule plan
    # return the reschedule plan
    pass


class GlobalScheduleExecutor(AutonomousMonitor, metaclass=Singleton):

  def __init__(self, scheduler, interval=5000):
    super(GlobalScheduleExecutor, self).__init__(interval)
    self.__contr_mgr = container.manager.ContainerManager()
    self.__cluster_mgr = cluster.manager.ClusterManager()
    self.__scheduler = scheduler

  async def schedule(self, contr):
    agents = await self.get_agents()
    plan = await self.__scheduler.schedule(contr, agents)
    for c in plan.containers:
      await self._provision_container(c)

  async def callback(self):
    agents = await self.get_agents()
    contrs = await self._get_containers()
    if not contrs: return
    plan = await self.__scheduler.reschedule(contrs, agents)
    for c in plan.containers:
      await self._provision_container(c)

  async def get_agents(self):
    return await self.__cluster_mgr.get_cluster(0)

  async def _get_containers(self, **kwargs):
    status, contrs, err = await self.__contr_mgr.get_containers(**kwargs)
    if err:
      self.logger.error(err)
    return contrs

  async def _provision_container(self, contr):
    await self.__contr_mgr.save_container(contr)
    status, contr, err = await self.__contr_mgr.provision_container(contr)
    if err:
      self.logger.error(err)


class DefaultGlobalScheduler(GlobalScheduler):

  async def schedule(self, contr, agents):
    pass

  async def reschedule(self, contrs, agents):
    pass 

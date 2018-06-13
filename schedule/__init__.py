import sys
import importlib

import cluster
import appliance

from abc import ABCMeta

from commons import AutonomousMonitor, Singleton, Loggable
from container.manager import ContainerManager
from config import config


def get_scheduler():
  try:
    sched_mod = '.'.join(config.pivot.scheduler.split('.')[:-1])
    sched_class = config.pivot.scheduler.split('.')[-1]
    return getattr(importlib.import_module(sched_mod), sched_class)
  except Exception as e:
    sys.stderr.write(str(e) + '\n')
    from schedule.default import DefaultApplianceScheduler
    return DefaultApplianceScheduler


class ApplianceScheduleNegotiator(AutonomousMonitor):

  def __init__(self, app_id, interval=3000):
    super(ApplianceScheduleNegotiator, self).__init__(interval)
    self.__app_id = app_id
    self.__executor = ApplianceScheduleExecutor()
    self.__scheduler = get_scheduler()()
    self.__cluster_mgr = cluster.manager.ClusterManager()
    self.__app_mgr = appliance.manager.ApplianceManager()

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
    agents = await self.__cluster_mgr.get_cluster(ttl=0)
    # contact the scheduler for new schedule
    sched = await self.__scheduler.schedule(app, agents)
    self.logger.debug('Containers to be scheduled: %s'%[c.id for c in sched.containers])
    # if the scheduling is done
    if sched.done:
      self.logger.info('Scheduling is done for appliance %s'%self.__app_id)
      self.stop()
      return
    # execute the new schedule
    await self.__executor.execute(sched)


class ApplianceScheduleExecutor(Loggable, metaclass=Singleton):

  def __init__(self):
    self.__contr_mgr = ContainerManager()

  async def execute(self, sched):
    for c in sched.containers:
      _, msg, err = await self.__contr_mgr.provision_container(c)
      if err:
        self.logger.error(err)
      self.logger.info('Container %s is being provisioned'%c.id)


class Schedule:

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


class ApplianceScheduler(Loggable, metaclass=ABCMeta):

  async def schedule(self, app, agents):
    raise NotImplemented


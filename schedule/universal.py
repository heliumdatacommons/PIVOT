import volume
import cluster.manager

from tornado.gen import multi

from schedule import SchedulePlan
from schedule.task import Task, ServiceTask, JobTask
from schedule.manager import ServiceTaskManager, JobTaskManager
from schedule.local import ApplianceSchedulerRunner
from commons import Singleton, Loggable, AutonomousMonitor, SynchronizedQueue
from config import get_global_scheduler


class GlobalSchedulerRunner(AutonomousMonitor, metaclass=Singleton):

  def __init__(self, interval=3000):
    super(GlobalSchedulerRunner, self).__init__(interval)
    self.__srv_mgr = ServiceTaskManager()
    self.__job_mgr = JobTaskManager()
    self.__vol_mgr = volume.manager.VolumeManager()
    self.__cluster_mgr = cluster.manager.ClusterManager()
    self.__scheduler = sched = get_global_scheduler()
    self.logger.info('Global scheduler: %s'%sched.__class__.__name__)
    self.__local_schedulers = {}
    self.__task_q = TaskQueue()
    self.__vol_q = VolumeQueue()

  def get_appliance_scheduler(self, app_id):
    return self.__local_schedulers.get(app_id)

  def register_local_scheduler(self, app_id, app_scheduler):
    assert isinstance(app_id, str)
    assert isinstance(app_scheduler, ApplianceSchedulerRunner)
    self.__local_schedulers[app_id] = app_scheduler

  def deregister_local_scheduler(self, app_id):
    return self.__local_schedulers.pop(app_id, None)

  async def submit(self, plan):
    """

    :param plan: schedule.base.SchedulePlan

    """
    assert isinstance(plan, SchedulePlan)
    task_q, vol_q = self.__task_q, self.__vol_q
    await task_q.enqueue(*plan.tasks)
    await vol_q.enqueue(*plan.volumes)

  async def callback(self):
    cluster_mgr, task_q, vol_q = self.__cluster_mgr, self.__task_q, self.__vol_q
    global_sched = self.__scheduler
    agents = await cluster_mgr.get_agents(0)
    tasks = await task_q.dequeue_all()
    vols = await vol_q.dequeue_all()
    # self.logger.debug('%d tasks, %d volumes to schedule' % (len(tasks), len(vols)))
    ensembles = {app_id: sched.ensemble for app_id, sched in self.__local_schedulers.items()}
    self._add_predecessor_placements(tasks, ensembles)
    tasks, vols = global_sched.schedule(tasks, vols, agents, ensembles)
    for t in tasks:
      if t.schedule_hints.placement:
        self.logger.debug('Task %s is placed on %s'%(t, t.schedule_hints.placement))
    await multi([self._provision_volume(v) for v in vols])
    await multi([self._provision_task(t) for t in tasks])

  @staticmethod
  def _add_predecessor_placements(tasks, ensembles):
    for t in tasks:
      ensemble = ensembles[t.appliance.id]
      from schedule.task import TaskEnsemble
      assert isinstance(ensemble, TaskEnsemble)
      preds = ensemble.get_predecessors(t.id)
      if preds:
        placements = list(set([p.placement for p in preds]))
        data_src = dict(DATA_SRC_CLOUD=','.join([p.cloud for p in placements]),
                        DATA_SRC_REGION=','.join([p.region for p in placements]),
                        DATA_SRC_ZONE=','.join([p.zone for p in placements]),
                        DATA_SRC_HOST=','.join([p.host for p in placements]))
        t.env = data_src

  async def _provision_task(self, task):
    assert isinstance(task, Task)
    srv_mgr, job_mgr = self.__srv_mgr, self.__job_mgr
    if isinstance(task, ServiceTask):
      status, _, err = await srv_mgr.launch_service_task(task)
      if status != 200:
        self.logge.error(err)
    elif isinstance(task, JobTask):
      status, _, err = await job_mgr.launch_job_task(task)
      if status != 200:
        self.logger.error(err)

  async def _provision_volume(self, vol):
    self.logger.info("Volume '%s' is being provisioned"%vol.id)
    status, _, err = await self.__vol_mgr.provision_volume(vol)
    if status != 200:
      self.logger.error(err)


class TaskQueue(SynchronizedQueue):

  async def enqueue(self, *tasks):
    assert all([isinstance(t, Task) for t in tasks])
    await super(TaskQueue, self).enqueue(*tasks)


class VolumeQueue(SynchronizedQueue):

  async def enqueue(self, *vols):
    assert all([isinstance(v, volume.PersistentVolume) for v in vols])
    await super(VolumeQueue, self).enqueue(*vols)


class GlobalSchedulerBase(Loggable):

  def schedule(self, tasks, volumes, agents, ensembles):
    """

    :param tasks: Iterable[schedule.task.Task]
    :param volumes: Iterable[volume.PersistentVolume]
    :param agents: Iterable[cluster.Agent]
    :param ensembles: dict[str: schedule.task.TaskEnsemble], key is appliance ID
    :return: Iterable[schedule.task.Task], Iterable[volume.PersistentVolume]
    """
    raise NotImplemented


class DefaultGlobalScheduler(GlobalSchedulerBase):
  """
  Delegate the scheduling totally to Mesos scheduling

  """

  def schedule(self, tasks, volumes, agents, ensembles):
    return tasks, volumes



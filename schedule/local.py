import cluster
import appliance
import container

from abc import ABCMeta
from tornado.gen import multi
from collections import Iterable

from schedule import SchedulePlan
from schedule.task import Task, ServiceTask, JobTask, TaskEnsemble, TaskState
from schedule.manager import GeneralTaskManager, ServiceTaskManager, JobTaskManager
from commons import AutonomousMonitor, Loggable


class ApplianceSchedulerRunner(AutonomousMonitor):

  def __init__(self, app, scheduler, interval=3000):
    super(ApplianceSchedulerRunner, self).__init__(interval)
    assert isinstance(app, appliance.Appliance)
    self.__app = app
    assert isinstance(scheduler, ApplianceScheduler)
    self.__local_sched = scheduler
    self.__app_mgr = appliance.manager.ApplianceManager()
    self.__contr_mgr = container.manager.ContainerManager()
    self.__cluster_mgr = cluster.manager.ClusterManager()
    self.__task_mgr = GeneralTaskManager()
    self.__srv_mgr = ServiceTaskManager()
    self.__job_mgr = JobTaskManager()
    from schedule.universal import GlobalSchedulerRunner
    self.__global_scheduler = GlobalSchedulerRunner()
    self.__ensemble = None

  @property
  def ensemble(self):
    return self.__ensemble

  async def callback(self):
    app_mgr, cluster_mgr = self.__app_mgr, self.__cluster_mgr
    global_sched, local_sched = self.__global_scheduler, self.__local_sched
    app, ensemble = self.__app, self.__ensemble
    if ensemble:
      await self._update_task_states(ensemble.current_tasks)
    else:
      self.__ensemble = ensemble = TaskEnsemble(app)
    agents = await cluster_mgr.get_agents(0)
    plan = await local_sched.schedule(ensemble, agents)
    if plan.done:
      self.logger.debug("Scheduling for appliance '%s' has finished"%app.id)
      self.stop()
      return
    await global_sched.submit(plan)

  async def _update_task_states(self, tasks):
    assert isinstance(tasks, Iterable) and all([isinstance(t, Task) for t in tasks])
    task_mgr, srv_mgr, job_mgr = self.__task_mgr, self.__srv_mgr, self.__job_mgr
    contr_mgr = self.__contr_mgr
    general_tasks, srv_tasks, job_tasks = [], [], []
    for t in tasks:
      if t.mesos_task_id:
        general_tasks += t,
      elif isinstance(t, ServiceTask):
        srv_tasks += t,
      elif isinstance(t, JobTask):
        job_tasks += t,
    for status, t, err in await multi([task_mgr.update_task(t) for t in general_tasks]):
      if status != 200:
        self.logger.error(err)
    for status, t, err in await multi([srv_mgr.update_service_task(t) for t in srv_tasks]):
      if status != 200:
        self.logger.error(err)
    for status, t, err in await multi([job_mgr.update_job_task(t) for t in job_tasks]):
      if status != 200:
        self.logger.error(err)
    await multi([job_mgr.delete_job_task(t) for t in tasks
                 if isinstance(t, JobTask) and t.state == TaskState.TASK_FINISHED])
    # persist task updates to DB
    for c in set([t.container for t in tasks]):
      await contr_mgr.save_container(c)


class ApplianceScheduler(Loggable, metaclass=ABCMeta):

  def __init__(self, config={}):
    self.__config = dict(config)

  @property
  def config(self):
    return dict(self.__config)

  async def schedule(self, ensemble, agents):
    """
    Caution: the parameters should not be overridden by schedulers that extend
    this class, otherwise inconsistency of appliance/cluster info will be caused.

    :param ensemble: schedule.base.TaskEnsemble
    :param agents: cluster.Agent
    :return: schedule.SchedulePlan

    """
    raise NotImplemented


class DefaultApplianceScheduler(ApplianceScheduler):
  """
  Delegate to the Mesos opportunistic scheduler

  """

  def __init__(self, *args, **kwargs):
    super(DefaultApplianceScheduler, self).__init__(*args, **kwargs)

  async def schedule(self, ensemble, agents):
    """

    :param ensemble: schedule.base.TaskEnsemble
    :param agents: cluster.Agent
    :return: schedule.SchedulePlan

    """
    assert isinstance(ensemble, TaskEnsemble)
    assert isinstance(agents, Iterable) and all([isinstance(a, cluster.Agent) for a in agents])
    plan = SchedulePlan()
    cur_tasks, ready_tasks = ensemble.current_tasks, ensemble.ready_tasks
    if len(cur_tasks) == 0 and len(ready_tasks) == 0:
      plan.set_done()
      return plan
    self.logger.debug('Tasks to schedule: %s'%[c.id for c in ready_tasks])
    for t in ready_tasks:
      contr = t.container
      assert isinstance(contr, container.Container)
      t.schedule_hints = contr.schedule_hints
    plan.add_tasks(*ready_tasks)

    app = ensemble.appliance
    vols_declared = {v.id: v for v in app.volumes}
    ready_vols = set([v.src for t in ready_tasks for v in t.container.persistent_volumes
                      if v.type == container.ContainerVolumeType.PERSISTENT
                      and v.src in vols_declared
                      and not vols_declared[v.src].is_active])
    ready_vols = [vols_declared[vid] for vid in ready_vols]
    self.logger.debug('Volumes to schedule: %s'%ready_vols)
    plan.add_volumes(*ready_vols)
    return plan
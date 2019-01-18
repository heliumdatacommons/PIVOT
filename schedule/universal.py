import volume.manager

from tornado.gen import multi

from schedule import SchedulePlan
from schedule.task import Task, ServiceTask, JobTask
from schedule.manager import ServiceTaskManager, JobTaskManager
from schedule.local import ApplianceSchedulerRunner
from commons import Singleton, Loggable


class GlobalSchedulerRunner(Loggable, metaclass=Singleton):

  def __init__(self):
    super(GlobalSchedulerRunner, self).__init__()
    self.__srv_mgr = ServiceTaskManager()
    self.__job_mgr = JobTaskManager()
    self.__vol_mgr = volume.manager.VolumeManager()
    self.__schedulers = {}

  def get_appliance_scheduler(self, app_id):
    return self.__schedulers.get(app_id)

  def register(self, app_id, app_scheduler):
    assert isinstance(app_id, str)
    assert isinstance(app_scheduler, ApplianceSchedulerRunner)
    self.__schedulers[app_id] = app_scheduler

  def deregister(self, app_id):
    return self.__schedulers.pop(app_id, None)

  async def submit(self, plan):
    """

    :param plan: schedule.base.SchedulePlan

    """
    assert isinstance(plan, SchedulePlan)
    await multi([self._provision_volume(v) for v in plan.volumes])
    await multi([self._provision_task(t) for t in plan.tasks])

  async def _provision_task(self, task):
    assert isinstance(task, Task)
    srv_mgr, job_mgr = self.__srv_mgr, self.__job_mgr
    if isinstance(task, ServiceTask):
      await srv_mgr.launch_service_task(task)
    elif isinstance(task, JobTask):
      await job_mgr.launch_job_task(task)

  async def _provision_volume(self, vol):
    self.logger.info("Volume '%s' is being provisioned"%vol.id)
    status, _, err = await self.__vol_mgr.provision_volume(vol)
    if status != 200:
      self.logger.error(err)




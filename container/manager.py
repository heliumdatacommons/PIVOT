from tornado.gen import multi

import appliance.manager

from config import config
from commons import MongoClient, Manager
from container import Container, ContainerType
from schedule.manager import ServiceTaskManager, JobTaskManager


class ContainerManager(Manager):

  def __init__(self):
    self.__contr_db = ContainerDBManager()
    self.__srv_mgr = ServiceTaskManager()
    self.__job_mgr = JobTaskManager()

  async def get_container(self, app_id, contr_id, full_blown=False):
    db = self.__contr_db
    status, contr, err = await db.get_container(app_id, contr_id)
    if status == 404:
      return status, contr, err
    app_mgr = appliance.manager.ApplianceManager()
    status, contr.appliance, err = await app_mgr.get_appliance(app_id)
    return 200, contr, None

  async def get_containers(self, **filters):
    db = self.__contr_db
    contrs = await db.get_containers(**filters)
    app_mgr = appliance.manager.ApplianceManager()
    for c in contrs:
      _, c.appliance, _ = await app_mgr.get_appliance(c.appliance)
    return 200, contrs, None

  async def create_container(self, data):
    db = self.__contr_db
    status, contr, err = Container.parse(data)
    if status != 200:
      return status, None, err
    status, _, _ = await db.get_container(contr.appliance, contr.id)
    if status == 200:
      return 409, None, "Container '%s' already exists"%contr.id
    return 201, contr, None

  async def delete_container(self, app_id, contr_id):
    db, srv_mgr, job_mgr = self.__contr_db, self.__srv_mgr, self.__job_mgr
    status, contr, err = await self.get_container(app_id, contr_id)
    if status == 404:
      return status, contr, err
    tasks = contr.tasks
    if contr.type == ContainerType.SERVICE:
      res = await multi([srv_mgr.delete_service_task(t) for t in tasks])
      failed = []
      for i, (status, _, err) in enumerate(res):
        if status != 200:
          self.logger.error(err)
          failed += tasks[i].id,
      if failed:
        return 207, None, "Failed to delete service tasks: %s"%failed
    elif contr.type == ContainerType.JOB:
      failed = []
      res = await multi([job_mgr.delete_job_task(t) for t in tasks])
      for i, (status, _, err) in enumerate(res):
        if status != 200:
          self.logger.error(err)
          failed += tasks[i].id,
      if failed:
        return 207, None, "Failed to delete job tasks: %s"%failed
    await db.delete_containers(appliance=app_id, id=contr_id)
    return 200, "Container '%s' is being deleted"%contr, None

  async def delete_containers(self, **filters):
    db, failed = self.__contr_db, []
    contrs = await db.get_containers(**filters)
    for i, (status, _, err) in enumerate(await multi([self.delete_container(c) for c in contrs])):
      if status != 200:
        self.logger.error(err)
        failed += contrs[i],
    if failed:
      return 207, None, "Failed to delete containers %s"%failed
    return 200, "Containers matching %s have been deleted"%filters, None

  async def save_container(self, contr, upsert=False):
    assert isinstance(contr, Container)
    await self.__contr_db.save_container(contr, upsert=upsert)


class ContainerDBManager(Manager):

  def __init__(self):
    self.__contr_col = MongoClient()[config.db.name].container

  async def get_container_by_virtual_ip_address(self, ip_addr):
    return await self._get_container(**{'deployment.ip_addresses': ip_addr})

  async def get_container(self, app_id, contr_id):
    return await self._get_container(id=contr_id, appliance=app_id)

  async def get_containers(self, **filters):
    return [Container.parse(c, False)[1] async for c in self.__contr_col.find(filters)]

  async def save_container(self, contr, upsert=True):
    assert contr.appliance is not None and isinstance(contr.appliance, appliance.Appliance)
    await self.__contr_col.replace_one(dict(id=contr.id, appliance=contr.appliance.id),
                                       contr.to_save(), upsert=upsert)

  async def delete_container(self, contr):
    await self.__contr_col.delete_one(dict(id=contr.id, appliance=contr.appliance))
    return 200, "Container '%s' has been deleted"%contr, None

  async def delete_containers(self, **filters):
    await self.__contr_col.delete_many(filters)
    return 200, "Containers matching '%s' have been deleted"%filters, None

  async def _get_container(self, **filters):
    contr = await self.__contr_col.find_one(filters)
    if not contr:
      return 404, None, "Container matching '%s' is not found"%filters
    return Container.parse(contr, False)
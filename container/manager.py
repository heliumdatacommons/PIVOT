import datetime

from datetime import timedelta

from container.base import Container, ContainerType, ContainerState
from container.service import Service
from container.job import Job
from cluster.base import Cluster
from util import SecureAsyncHttpClient
from util import Singleton, MotorClient, Loggable


class ContainerManager(Loggable, metaclass=Singleton):

  CONTAINER_REC_TTL = timedelta(seconds=5)

  def __init__(self, config):
    self.__config = config
    self.__contr_col = MotorClient().requester.container
    self.__http_cli = SecureAsyncHttpClient(config)
    self.__cluster = Cluster(config)

  async def get_container(self, app_id, contr_id):
    status, contr, err = await self._get_container_from_db(app_id, contr_id)
    if status == 404:
      return status, contr, err
    if not contr.last_update or \
        datetime.datetime.now(tz=None) - contr.last_update > self.CONTAINER_REC_TTL:
      status, contr, err = await self._get_updated_container(contr)
      if status == 404 and contr.state != ContainerState.SUBMITTED:
        self.logger.info("Deleted ghost container: %s"%contr)
        await self._delete_container_from_db(contr)
        return 404, None, err
      if status == 200:
        await self._save_container_to_db(contr, False)
      elif status != 404:
        self.logger.error("Failed to update container '%s'"%contr)
        self.logger.error(err)
    return 200, contr, None

  async def get_containers(self, app_id, **kwargs):
    contrs = await self._get_containers_from_db(appliance=app_id, **kwargs)
    contrs_to_del, contrs_to_update = [], [],
    cur_time = datetime.datetime.now(tz=None)
    for c in contrs:
      if c.last_update and cur_time - c.last_update <= self.CONTAINER_REC_TTL:
        continue
      status, c, err = await self._get_updated_container(c)
      if status == 404 and c.state != ContainerState.SUBMITTED:
        contrs_to_del.append(c)
      if status == 200:
        contrs_to_update.append(c)
    if contrs_to_del:
      filters = dict(id={'$in': [c.id for c in contrs_to_del]}, appliance=app_id)
      status, msg, err = await self._delete_containers_from_db(**filters)
      if err:
        self.logger.error(err)
      else:
        self.logger.info(msg)

    for c in contrs_to_update:
      await self._save_container_to_db(c, upsert=False)
    return 200, contrs, None

  async def create_container(self, data):
    status, _, _ = await self._get_container_from_db(data['appliance'], data['id'])
    if status == 200:
      return 409, None, "Container '%s' already exists"%data['id']
    try:
      contr = self.instantiate_container(data)
      await self._save_container_to_db(contr, True)
      return 201, contr, None
    except ValueError as e:
      return 422, None, str(e)

  async def delete_container(self, app_id, contr_id):
    status, contr, err = await self._get_container_from_db(app_id, contr_id)
    if status == 404:
      return status, None, err
    if contr.state != ContainerState.SUBMITTED:
      if contr.type == ContainerType.SERVICE:
        status, msg, err = await self._delete_service(contr)
        if err:
          self.logger.error(err)
        else:
          self.logger.info(msg)
      elif contr.type == ContainerType.JOB:
        _, _, kill_job_err = await self._kill_job_tasks(contr)
        if kill_job_err:
          self.logger.error(kill_job_err)
        _, _, del_job_err = await self._delete_job(contr)
        if del_job_err:
          self.logger.error(del_job_err)
    await self._delete_container_from_db(contr)
    return 200, "Container '%s' has been deleted"%contr, None

  async def delete_containers(self, **filters):
    failed = []
    for c in await self._get_containers_from_db(**filters):
      status, msg, err = await self.delete_container(c.appliance, c.id)
      if err:
        if status != 404:
          self.logger.error(err)
          failed += [c.id]
        self.logger.info("Container '%s' no more exists"%c)
      else:
        self.logger.info(msg)
    if failed:
      return 207, None, "Failed to delete containers %s"%failed
    return 200, "Containers matching %s have been deleted"%filters, None

  async def provision_container(self, contr):
    assert isinstance(contr, Container)
    if contr.type == ContainerType.SERVICE:
      ### WARNING: if the service already exists, it will be overriden by the new
      ### container definition
      status, _, err = await self._provision_service(contr)
    elif contr.type == ContainerType.JOB:
      status, _, err = await self._provision_job(contr)
    if err:
      self.logger.debug('Failed to provision %s'%contr)
      return status, None, err
    return status, contr, None

  def instantiate_container(self, contr):
    if contr['type'] == ContainerType.SERVICE.value:
      return Service(**contr)
    if contr['type'] == ContainerType.JOB.value:
      return Job(**contr)
    raise ValueError("Unknown container type: %s"%contr['type'])

  async def _get_container_from_db(self, app_id, contr_id):
    contr = await self.__contr_col.find_one(dict(id=contr_id, appliance=app_id))
    if not contr:
      return 404, None, "Container '%s' is not found"%contr_id
    return 200, self.instantiate_container(contr), None

  async def _get_containers_from_db(self, **filters):
    return [self.instantiate_container(c) async for c in self.__contr_col.find(filters)]

  async def _save_container_to_db(self, contr, upsert=True):
    await self.__contr_col.replace_one(dict(id=contr.id, appliance=contr.appliance),
                                       contr.to_save(), upsert=upsert)

  async def _delete_container_from_db(self, contr):
    await self.__contr_col.delete_one(dict(id=contr.id, appliance=contr.appliance))
    return 200, "Container '%s' has been deleted"%contr, None

  async def _delete_containers_from_db(self, **filters):
    await self.__contr_col.delete_many(filters)
    return 200, "Containers matching '%s' have been deleted"%filters, None

  async def _get_updated_container(self, contr):
    assert isinstance(contr, Container)
    self.logger.info('Update container info: %s'%contr)
    if contr.type == ContainerType.SERVICE:
      status, contr, err = await self._get_updated_service(contr)
    elif contr.type == ContainerType.JOB:
      status, contr, err = await self._get_update_job(contr)
    else:
      errmsg = "Unknown container type: %s"%contr.type
      self.logger.warn(errmsg)
      return 404, None, "Container '%s' is not found"%contr
    return status, contr, err

  async def _get_updated_service(self, service):
    url = '%s/apps%s'%(self.__config.url.service_scheduler, service)
    status, resp, err = await self.__http_cli.get(url)
    if status == 404:
      return status, service, "Service '%s' is not found"%service
    if status != 200:
      self.logger.debug(err)
      return status, service, err
    serv_info = Service.parse(resp, self.__cluster)
    service.state, service.endpoints = serv_info['state'], serv_info['endpoints']
    service.rack, service.host = serv_info['rack'], serv_info['host']
    self.logger.debug('Updated service %s' % service)
    return status, service, None

  async def _get_update_job(self, job):
    """
    TO BE IMPROVED

    :param contr:
    :return:
    """
    url = '%s/jobs/summary'%self.__config.url.job_scheduler
    status, body, err = await self.__http_cli.get(url)
    jobs = [j for j in body['jobs'] if j['name'] == str(job)]
    if not jobs:
      return 404, job, "Job '%s' is not found"%job
    if status != 200:
      self.logger.debug(err)
      return status, job, err
    job_info = Job.parse(jobs[0], self.__cluster)
    job.state = job_info['state']
    return status, job, None

  async def _provision_service(self, service):
    url = '%s/apps?force=true'%self.__config.url.service_scheduler
    body = dict(service.to_request())
    return await self.__http_cli.post(url, body)

  async def _delete_service(self, contr):
    return await self.__http_cli.delete('%s/apps%s?force=true'%(self.__config.url.service_scheduler, contr))

  async def _provision_job(self, job):
    return await self.__http_cli.post('%s/iso8601'%self.__config.url.job_scheduler, job.to_request())

  async def _kill_job_tasks(self, contr):
    return await self.__http_cli.delete('%s/task/kill/%s'%(self.__config.url.job_scheduler, contr))

  async def _delete_job(self, contr):
    return await self.__http_cli.delete('%s/job/%s'%(self.__config.url.job_scheduler, contr))



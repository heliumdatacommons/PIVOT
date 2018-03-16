import datetime

from datetime import timedelta

from container.service import Service
from container.job import Job
from cluster.base import Cluster
from util import SecureAsyncHttpClient
from util import Singleton, MotorClient, Loggable
from util import message, error


class ContainerManager(Loggable, metaclass=Singleton):

  CONTAINER_REC_TTL = timedelta(seconds=5)

  def __init__(self, config):
    self.__config = config
    self.__contr_col = MotorClient().requester.container
    self.__http_cli = SecureAsyncHttpClient(config)
    self.__cluster = Cluster(config)
    self.__url_bases = {
      'service': '%s/%s'%(config['dcos']['master_url'], config['dcos']['service_scheduler_route']),
      'job': '%s/%s'%(config['dcos']['master_url'], config['dcos']['job_scheduler_route'])
    }

  async def get_container(self, app_id, contr_id):
    status, contr, err = await self._get_container_from_db(app_id, contr_id)
    if status == 404:
      return status, contr, err
    if not contr.last_update or \
        datetime.datetime.now(tz=None) - contr.last_update > self.CONTAINER_REC_TTL:
      status, contr, err = await self._get_updated_container(contr)
      if status == 404:
        await self._delete_container_from_db(contr)
        return 404, None, error("Container '%s' is not found"%contr_id)
      if status == 200:
        await self._save_container_to_db(contr, False)
      else:
        self.logger.error("Failed to update container '%s'"%contr)
        self.logger.error(err)
    return 200, contr, None

  async def get_containers(self, app_id, **kwargs):
    contrs = await self._get_containers(**kwargs)
    if not contrs:
      return 404, None, error("Containers of appliance '%s' are not found"%app_id)
    contrs_to_del, contrs_to_update = [], [],
    cur_time = datetime.datetime.now(tz=None)
    for c in contrs:
      if c.last_update and cur_time - c.last_update <= self.CONTAINER_REC_TTL:
        continue
      status, c, err = await self._get_updated_container(c)
      if status == 404:
        contrs_to_del.append(c)
      if status == 200:
        contrs_to_update.append(c)
    await self._delete_containers_from_db(contrs_to_del)
    for c in contrs_to_update:
      await self._save_container_to_db(c, upsert=False)
    if contrs_to_del == contrs:
      return 404, None, error("Containers of appliance '%s' are not found"%app_id)
    return 200, contrs, None

  async def create_container(self, data):
    status, _, _ = await self._get_container_from_db(data['appliance'], data['id'])
    if status == 200:
      return 409, None, error("Container '%s' already exists"%data['id'])
    contr = self._instantiate_container(data)
    if not contr:
      errmsg = 'Unknown container type: %s'%contr.type
      self.logger.error(errmsg)
      return 400, None, errmsg
    await self._save_container_to_db(contr, True)
    return 200, contr, None

  async def delete_container(self, app_id, contr_id):
    status, contr, err = await self._get_container_from_db(app_id, contr_id)
    if status == 404:
      return status, None, err
    if isinstance(contr, Service):
      _, _, err = await self._delete_service(contr)
      if err:
        self.logger.error(error)
    elif isinstance(contr, Job):
      _, _, kill_job_err = await self._kill_job_tasks(contr)
      if kill_job_err:
        self.logger.error(kill_job_err)
      _, _, del_job_err = await self._delete_job(contr)
      if del_job_err:
        self.logger.error(del_job_err)
    await self._delete_container_from_db(contr)
    return 200, message("Container '%s' has been deleted"%contr), None

  async def provision_container(self, contr):
    if isinstance(contr, Service):
      ### WARNING: if the service already exists, it will be overriden by the new
      ### container definition
      status, body, err = await self._provision_service(contr)
    elif isinstance(contr, Job):
      status, body, err = await self._provision_job(contr)
    if status != 200:
      self.logger.debug('Failed to provision %s'%contr)
      return status, None, err
    return status, contr, None

  async def deprovision_container(self, contr):
    if isinstance(contr, Service):
      status, msg, err = await self._delete_service(contr)
      if status == 200:
        self.logger.info("Service '%s' has been deleted")
      else:
        self.logger.debug(err)
      return 200, message("Service '%s' has been deprovisioned"), None
    elif isinstance(contr, Job):
      ### TO BE IMPROVED: delete the job until the job is not shown "running" in Chronos
      # kill all tasks of the job
      status, msg, err = await self._kill_job_tasks(contr)
      if status == 200:
        self.logger.info("Tasks of Job '%s' are being killed"%contr)
      else:
        self.logger.debug(err)
      # delete the job
      status, msg, err = await self._delete_job(contr)
      if status == 200:
        self.logger.info("Job '%s' has been deleted"%contr)
      else:
        self.logger.debug(err)
      return 200, message("Job '%s' has been deprovisioned"%contr), None
    else:
      errmsg = "Unknown container type: %s"%type(contr)
      self.logger.warn(errmsg)
      return 400, None, error(errmsg)

  async def _get_container_from_db(self, app_id, contr_id):
    contr = await self.__contr_col.find_one(dict(id=contr_id, appliance=app_id))
    if not contr:
      return 404, None, error("Container '%s' is not found"%(contr_id))
    return 200, self._instantiate_container(contr), None

  async def _get_containers(self, **filters):
    return [self._instantiate_container(c)
            async for c in self.__contr_col.find(filters)]

  async def _save_container_to_db(self, contr, upsert=True):
    await self.__contr_col.replace_one(dict(id=contr.id, appliance=contr.appliance),
                                       contr.to_save(), upsert=upsert)

  async def _delete_container_from_db(self, contr):
    await self.__contr_col.delete_many(dict(id=contr.id, appliance=contr.appliance))
    return 200, message("Container '%s' has been deleted"%contr), None

  async def _delete_containers_from_db(self, contrs):
    if not contrs: return
    contr_ids = [c.id for c in contrs]
    await self.__contr_col.delete_many(dict(id={'$in': contr_ids},
                                            appliance=contrs[0].appliance))
    return 200, message("Containers '%s' have been deleted"%contr_ids), None

  async def _delete_containers_of_appliance_from_db(self, app_id):
    await self.__contr_col.delete_many(dict(appliance=app_id))
    return 200, message("Containers of appliance '%s' have been deleted"%app_id), None

  async def _get_updated_container(self, contr):
    self.logger.info('Update container info: %s'%contr)
    if isinstance(contr, Service):
      status, contr, err = await self._get_updated_service(contr)
    elif isinstance(contr, Job):
      status, contr, err = await self._get_update_job(contr)
    else:
      errmsg = "Unknown container type: %s"%contr.type
      self.logger.warn(errmsg)
      return 404, None, "Container '%s' is not found"%contr
    return status, contr, err

  async def _get_updated_service(self, service):
    url = '%s/apps%s'%(self.__url_bases['service'], service)
    status, resp, err = await self.__http_cli.get(url)
    if status == 404:
      return status, service, "Service '%s' is not found"%service
    if status != 200:
      self.logger.debug(err)
      return status, service, err
    serv_info = Service.parse(resp, self.__cluster)
    service.state, service.endpoints = serv_info['state'], serv_info['endpoints']
    service.racks, service.hosts = serv_info['racks'], serv_info['hosts']
    self.logger.debug('Updated service %s' % service)
    return status, service, None

  async def _get_update_job(self, job):
    """
    TO BE IMPROVED

    :param contr:
    :return:
    """
    url = '%s/jobs/summary'%self.__url_bases['job']
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
    url = '%s/groups'%self.__url_bases['service']
    body = dict(id='/' + service.appliance, apps=[service.to_request()])
    return await self.__http_cli.put(url, body)

  async def _delete_service(self, contr):
    return await self.__http_cli.delete('%s/apps%s'%(self.__url_bases['service'], contr))

  async def _provision_job(self, job):
    return await self.__http_cli.post('%s/iso8601'%self.__url_bases['job'], job.to_request())

  async def _kill_job_tasks(self, contr):
    return await self.__http_cli.delete('%s/task/kill/%s'%(self.__url_bases['job'], contr))

  async def _delete_job(self, contr):
    return await self.__http_cli.delete('%s/job/%s'%(self.__url_bases['job'], contr))

  def _instantiate_container(self, contr):
    if contr['type'] == 'service':
      return Service(**contr)
    if contr['type'] == 'job':
      return Job(**contr)
    return None



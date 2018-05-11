import json
import datetime

from datetime import timedelta

from commons import APIManager, Singleton, MotorClient, Loggable
from container.base import Container, ContainerType, ContainerState, Endpoint
from cluster.manager import AgentDBManager
from config import config


class ContainerManager(Loggable, metaclass=Singleton):

  def __init__(self, contr_info_ttl=timedelta(seconds=3)):
    self.__service_api = ServiceAPIManager()
    self.__job_api = JobAPIManager()
    self.__contr_db = ContainerDBManager()
    self.__cluster_db = AgentDBManager()
    self.__contr_info_ttl = contr_info_ttl

  async def get_container(self, app_id, contr_id):
    status, contr, err = await self.__contr_db.get_container(app_id, contr_id)
    if status == 404:
      return status, contr, err
    if not contr.last_update or \
        datetime.datetime.now(tz=None) - contr.last_update > self.__contr_info_ttl:
      status, contr, err = await self._get_updated_container(contr)
      if status == 404 and contr.state != ContainerState.SUBMITTED:
        self.logger.info("Deleted ghost container: %s"%contr)
        await self.__contr_db.delete_container(contr)
        return 404, None, err
      if status == 200:
        contr.last_update = datetime.datetime.now(tz=None)
        await self.__contr_db.save_container(contr, False)
      elif status != 404:
        self.logger.error("Failed to update container '%s'"%contr)
        self.logger.error(err)
    return 200, contr, None

  async def get_containers(self, app_id, **kwargs):
    contrs = await self.__contr_db.get_containers(appliance=app_id, **kwargs)
    contrs_to_del, contrs_to_update = [], [],
    cur_time = datetime.datetime.now(tz=None)
    for c in contrs:
      if c.last_update and cur_time - c.last_update <= self.__contr_info_ttl:
        continue
      status, c, err = await self._get_updated_container(c)
      if status == 404 and c.state != ContainerState.SUBMITTED:
        contrs_to_del.append(c)
      if status == 200:
        contrs_to_update.append(c)
    if contrs_to_del:
      filters = dict(id={'$in': [c.id for c in contrs_to_del]}, appliance=app_id)
      status, msg, err = await self.__contr_db.delete_containers(**filters)
      if err:
        self.logger.error(err)
      else:
        self.logger.info(msg)
    for c in contrs_to_update:
      c.last_update = datetime.datetime.now(tz=None)
      await self.__contr_db.save_container(c, upsert=False)
    return 200, contrs, None

  async def create_container(self, data):
    status, _, _ = await self.__contr_db.get_container(data['appliance'], data['id'])
    if status == 200:
      return 409, None, "Container '%s' already exists"%data['id']
    status, contr, err = Container.parse(data)
    if err:
      return status, None, err
    await self.__contr_db.save_container(contr, True)
    return 201, contr, None

  async def delete_container(self, app_id, contr_id):
    status, contr, err = await self.__contr_db.get_container(app_id, contr_id)
    if status == 404:
      return status, None, err
    if contr.state != ContainerState.SUBMITTED:
      if contr.type == ContainerType.SERVICE:
        status, msg, err = await self.__service_api.deprovision_service(contr)
        if err:
          self.logger.error(err)
        else:
          self.logger.info(msg)
      elif contr.type == ContainerType.JOB:
        _, _, kill_job_err = await self.__job_api.kill_job(contr)
        if kill_job_err:
          self.logger.error(kill_job_err)
        _, _, del_job_err = await self.__job_api.delete_job(contr)
        if del_job_err:
          self.logger.error(del_job_err)
    await self.__contr_db.delete_container(contr)
    return 200, "Container '%s' is being deleted"%contr, None

  async def delete_containers(self, **filters):
    failed = []
    for c in await self.__contr_db.get_containers(**filters):
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
      status, _, err = await self.__service_api.provision_service(contr)
    elif contr.type == ContainerType.JOB:
      status, _, err = await self.__job_api.provision_job(contr)
    if err:
      self.logger.debug('Failed to provision %s'%contr)
      return status, None, err
    return status, contr, None

  async def _get_updated_container(self, contr):
    assert isinstance(contr, Container)
    self.logger.debug('Update container info: %s'%contr)
    if contr.type == ContainerType.SERVICE:
      status, raw_service, err = await self.__service_api.get_service_update(contr)
      if not err:
        parsed_srv = await self._parse_service_state(raw_service)
        contr.state, contr.endpoints = parsed_srv['state'], parsed_srv['endpoints']
        contr.rack, contr.host = parsed_srv['rack'], parsed_srv['host']
    elif contr.type == ContainerType.JOB:
      status, raw_job, err = await self.__job_api.get_job_update(contr)
      if not err:
        parsed_job = await self._parse_job_state(raw_job)
        contr.state = parsed_job['state']
    else:
      errmsg = "Unknown container type: %s"%contr.type
      self.logger.warn(errmsg)
      return 404, None, "Container '%s' is not found"%contr
    return status, contr, err

  async def _parse_service_state(self, body):
    if isinstance(body, str):
      body = json.loads(body.decode('utf-8'))
    body = body['app']
    tasks, min_capacity = body['tasks'], body['upgradeStrategy']['minimumHealthCapacity']
    # parse state
    state = ContainerState.determine_state([t['state'] for t in tasks], min_capacity)
    if state == ContainerState.RUNNING and body.get('healthChecks', []):
      tasks_healthy, tasks_unhealthy = body['tasksHealthy'], body['tasksUnhealthy']
      instances = body['instances']
      if tasks_healthy/instances < min_capacity:
        if tasks_healthy + tasks_unhealthy < instances:
          state = ContainerState.PENDING
        else:
          state = ContainerState.FAILED
    # parse endpoints
    endpoints, rack, host = [], None, None
    if state == ContainerState.RUNNING:
      for t in tasks:
        hosts = await self.__cluster_db.find_agents(hostname=t['host'])
        if not hosts: continue
        host, rack = hosts[0], hosts[0].attributes.get('rack', None)
        public_ip = host.attributes.get('public_ip', None)
        if not public_ip: continue
        if 'portDefinitions' in body:
          for i, p in enumerate(body['portDefinitions']):
            endpoints += [Endpoint(public_ip, p['port'], t['ports'][i], p['protocol'])]
        else:
          for i, p in enumerate(body['container']['portMappings']):
            endpoints += [Endpoint(public_ip, p['containerPort'], t['ports'][i],
                                   p['protocol'])]
    _, appliance, id = body['id'].split('/')
    return dict(id=id, appliance=appliance, state=state,
                rack=rack, host=host and host.hostname, endpoints=endpoints)

  async def _parse_job_state(self, body):
    ### TO BE IMPORVED: currently the body is the output of the job summary due to
    ### limitations of Chronos API. With that being said, endpoints are not yet supported
    ### for jobs.
    assert isinstance(body, dict)
    state = body['state'].lower().strip('1 ')
    if body['status'] in ('success', 'failure'):
      state = body['status']
    state = dict(running=ContainerState.RUNNING,
                 success=ContainerState.SUCCESS,
                 failure=ContainerState.FAILED,
                 queued=ContainerState.STAGING,
                 idle=ContainerState.PENDING).get(state, ContainerState.SUBMITTED)
    appliance, id = body['name'].split('.')
    return dict(id=id, appliance=appliance, state=state)


class ServiceAPIManager(APIManager):

  def __init__(self):
    super(ServiceAPIManager, self).__init__()

  async def get_service_update(self, service):
    api = config.marathon
    endpoint = '%s/apps%s'%(api.endpoint, service)
    status, body, err = await self.http_cli.get(api.host, api.port, endpoint)
    if status == 404:
      return status, service, "Service '%s' is not found"%service
    if status != 200:
      self.logger.debug(err)
      return status, service, err
    return status, body, err

  async def provision_service(self, service):
    api = config.marathon
    endpoint = '%s/apps'%api.endpoint
    body = dict(service.to_request())
    return await self.http_cli.post(api.host, api.port, endpoint, body)

  async def deprovision_service(self, contr):
    api = config.marathon
    endpoint = '%s/apps%s?force=true'%(api.endpoint, contr)
    return await self.http_cli.delete(api.host, api.port, endpoint)


class JobAPIManager(APIManager):

  def __init__(self):
    super(JobAPIManager, self).__init__()

  async def get_job_update(self, job):
    api = config.chronos
    endpoint = '%s/jobs/summary'%api.endpoint
    status, body, err = await self.http_cli.get(api.host, api.port, endpoint)
    jobs = [j for j in body['jobs'] if j['name'] == str(job)]
    if not jobs:
      return 404, job, "Job '%s' is not found"%job
    if status != 200:
      self.logger.debug(err)
      return status, job, err
    return status, jobs[0], None

  async def provision_job(self, job):
    api = config.chronos
    endpoint = '%s/iso8601'%api.endpoint
    body = dict(job.to_request())
    return await self.http_cli.post(api.host, api.port, endpoint, body)

  async def kill_job(self, contr):
    api = config.chronos
    endpoint = '%s/task/kill/%s'%(api.endpoint, contr)
    return await self.http_cli.delete(api.host, api.port, endpoint)

  async def delete_job(self, contr):
    api = config.chronos
    endpoint = '%s/job/%s'%(api.endpoint, contr)
    return await self.http_cli.delete(api.host, api.port, endpoint)


class ContainerDBManager(Loggable, metaclass=Singleton):

  def __init__(self):
    self.__contr_col = MotorClient().requester.container

  async def get_container(self, app_id, contr_id):
    contr = await self.__contr_col.find_one(dict(id=contr_id, appliance=app_id))
    if not contr:
      return 404, None, "Container '%s' is not found"%contr_id
    return Container.parse(contr)

  async def get_containers(self, **filters):
    return [Container.parse(c)[1] async for c in self.__contr_col.find(filters)]

  async def save_container(self, contr, upsert=True):
    await self.__contr_col.replace_one(dict(id=contr.id, appliance=contr.appliance),
                                       contr.to_save(), upsert=upsert)

  async def delete_container(self, contr):
    await self.__contr_col.delete_one(dict(id=contr.id, appliance=contr.appliance))
    return 200, "Container '%s' has been deleted"%contr, None

  async def delete_containers(self, **filters):
    await self.__contr_col.delete_many(filters)
    return 200, "Containers matching '%s' have been deleted"%filters, None


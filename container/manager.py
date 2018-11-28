import json
import datetime

import appliance.manager

from datetime import timedelta

from config import config
from commons import MongoClient
from commons import APIManager, Manager
from cluster.manager import AgentDBManager
from container import Container, ContainerType, ContainerState, Endpoint, ContainerDeployment


class ContainerManager(Manager):

  def __init__(self):
    self.__service_api = ServiceAPIManager()
    self.__job_api = JobAPIManager()
    self.__contr_db = ContainerDBManager()
    self.__cluster_db = AgentDBManager()

  async def get_container(self, app_id, contr_id, ttl=0, full_blown=False):
    status, contr, err = await self.__contr_db.get_container(app_id, contr_id)
    if status == 404:
      return status, contr, err
    if not contr.last_update or \
        datetime.datetime.now(tz=None) - contr.last_update > timedelta(seconds=ttl):
      status, contr, err = await self._get_updated_container(contr)
      if status == 404 and contr.state != ContainerState.SUBMITTED:
        self.logger.info("Deleted ghost container: %s"%contr)
        await self.__contr_db.delete_container(contr)
        return 404, None, err
      if status == 200:
        contr.last_update = datetime.datetime.now(tz=None)
        await self.save_container(contr)
      elif status != 404:
        self.logger.error("Failed to update container '%s'"%contr)
        self.logger.error(err)
    if full_blown:
      app_mgr = appliance.manager.ApplianceManager()
      status, contr.appliance, err = await app_mgr.get_appliance(app_id)
    return 200, contr, None

  async def get_containers(self, ttl=0, full_blown=False, **filters):
    contrs = await self.__contr_db.get_containers(**filters)
    contrs_to_del, contrs_to_update = [], [],
    cur_time = datetime.datetime.now(tz=None)
    for c in contrs:
      if c.last_update and cur_time - c.last_update <= timedelta(seconds=ttl):
        continue
      status, c, err = await self._get_updated_container(c)
      if status == 404 and c.state != ContainerState.SUBMITTED:
        contrs_to_del.append(c)
      if status == 200:
        contrs_to_update.append(c)
    if contrs_to_del:
      filters = dict(id={'$in': [c.id for c in contrs_to_del]},
                     appliance=contrs_to_del[0].appliance)
      status, msg, err = await self.__contr_db.delete_containers(**filters)
      if err:
        self.logger.error(err)
      else:
        self.logger.info(msg)
    for c in contrs_to_update:
      c.last_update = datetime.datetime.now(tz=None)
      await self.save_container(c, upsert=False)
    if full_blown:
      app_mgr = appliance.manager.ApplianceManager()
      for c in contrs:
        _, c.appliance, _ = await app_mgr.get_appliance(c.appliance)
    return 200, contrs, None

  async def create_container(self, data):
    status, contr, err = Container.parse(data)
    if status != 200:
      return status, None, err
    status, _, _ = await self.__contr_db.get_container(contr.appliance, contr.id)
    if status == 200:
      return 409, None, "Container '%s' already exists"%contr.id
    await self.save_container(contr, True)
    return 201, contr, None

  async def delete_container(self, app_id, contr_id):
    status, contr, err = await self.__contr_db.get_container(app_id, contr_id)
    if status == 404:
      return status, contr, err
    if contr.type == ContainerType.SERVICE:
      status, msg, err = await self.__service_api.deprovision_service(contr)
      if status == 404:
        await self.__contr_db.delete_container(contr)
      elif status != 200:
        self.logger.error(err)
      else:
        self.logger.info(msg)
    elif contr.type == ContainerType.JOB:
      status, _, kill_job_err = await self.__job_api.kill_job(contr)
      if status != 404 and kill_job_err:
        self.logger.error(kill_job_err)
      status, _, del_job_err = await self.__job_api.delete_job(contr)
      if status != 404 and del_job_err:
        self.logger.error(del_job_err)
      if status == 404:
        await self.__contr_db.delete_container(contr)
    await self.__contr_db.delete_containers(appliance=app_id, id=contr_id)
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
    """

    :param contr: container.Container

    """
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

  async def save_container(self, contr, upsert=False):
    await self.__contr_db.save_container(contr, upsert=upsert)

  async def _get_updated_container(self, contr):
    assert isinstance(contr, Container)
    self.logger.debug('Update container info: %s'%contr)
    if contr.type == ContainerType.SERVICE:
      status, raw_service, err = await self.__service_api.get_service_update(contr)
      if not err:
        parsed_srv = await self._parse_service_state(raw_service)
        contr.state, contr.endpoints = parsed_srv['state'], parsed_srv['endpoints']
        contr.deployment = parsed_srv['deployment']
    elif contr.type == ContainerType.JOB:
      status, raw_job, err = await self.__job_api.get_job_update(contr)
      if not err:
        parsed_job = await self._parse_job_state(raw_job)
        contr.state, contr.deployment = parsed_job['state'], parsed_job['deployment']
    else:
      err = "Unknown container type: %s"%contr.type
      self.logger.warn(err)
      return 400, None, err
    # add descriptive names to the endpoints
    for i, p in enumerate(contr.ports):
      if i >= len(contr.endpoints): break
      contr.endpoints[i].name = p.name
    return status, contr, err

  async def _parse_service_state(self, body):
    if isinstance(body, str):
      body = json.loads(str(body, 'utf-8'))
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
    endpoints, deployment = [], ContainerDeployment()
    if state == ContainerState.RUNNING:
      for t in tasks:
        # parse endpoints
        hosts = await self.__cluster_db.find_agents(hostname=t['host'])
        if not hosts: continue
        host = hosts[0]
        hostname = host.attributes.get('fqdn') or host.attributes.get('public_ip')
        if not hostname:
          continue
        if 'portDefinitions' in body:
          for i, p in enumerate(body['portDefinitions']):
            endpoints += [Endpoint(hostname, p['port'], t['ports'][i], p['protocol'])]
        else:
          for i, p in enumerate(body['container']['portMappings']):
            endpoints += [Endpoint(hostname, p['containerPort'], t['ports'][i], p['protocol'])]
        # parse virtual IP addresses
        for ip in t['ipAddresses']:
          if ip['protocol'] != 'IPv4':
            continue
          deployment.add_ip_address(ip['ipAddress'])
        deployment.placement.host = host.hostname
        deployment.placement.cloud = host.attributes.get('cloud')
        deployment.placement.region = host.attributes.get('region')
        deployment.placement.zone = host.attributes.get('zone')
    _, appliance, id = body['id'].split('/')
    return dict(id=id, appliance=appliance, state=state,
                endpoints=endpoints, deployment=deployment)

  async def _parse_job_state(self, body):
    assert isinstance(body, dict)

    def get_n_repeats(schedule):
      n_repeats_str = schedule.split('/')[0].strip('R')
      return int(n_repeats_str) if len(n_repeats_str) > 0 else -1

    deployment = ContainerDeployment()
    res = dict(**body, state=ContainerState.PENDING, deployment=deployment)
    if 'task' not in body or not body['task']:
      return res
    task, schedule = body['task'], body['schedule']
    res['state'] = dict(TASK_STARTING=ContainerState.RUNNING,
                        TASK_RUNNING=ContainerState.RUNNING,
                        TASK_FINISHED=ContainerState.SUCCESS,
                        TASK_FAILED=ContainerState.FAILED,
                        TASK_LOST=ContainerState.FAILED,
                        TASK_ERROR=ContainerState.FAILED,
                        TASK_STAGING=ContainerState.STAGING,
                        TASK_KILLING=ContainerState.KILLED,
                        TASK_KILLED=ContainerState.KILLED).get(task.get('state'),
                                                               ContainerState.SUBMITTED)
    if res['state'] == ContainerState.SUCCESS and get_n_repeats(body['schedule']) != 0:
      res['state'] = ContainerState.RUNNING
    hosts = await self.__cluster_db.find_agents(id=task.get('slave_id'))
    if not hosts:
      if task.get('slave_id'):
        self.logger.warning('Unrecognized agent ID: %s'%task.get('slave_id'))
      res.update(state=ContainerState.PENDING)
      return res
    host = hosts[0]
    deployment.placement.host = host.hostname
    deployment.cloud = host.attributes.get('cloud')
    deployment.region = host.attributes.get('region')
    deployment.zone = host.attributes.get('zone')
    deployment.add_ip_address(host.hostname)
    return res


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
    endpoint = '%s/apps?force=true'%api.endpoint
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
    chronos = config.chronos
    endpoint = '%s/job/%s'%(chronos.endpoint, job)
    status, body, err = await self.http_cli.get(chronos.host, chronos.port, endpoint)
    if err:
      return status, None, err
    task_id = body['taskId']
    job = dict(id=job.id, appliance=job.appliance, schedule=body['schedule'])
    if not task_id:
      return status, job, None
    mesos = config.mesos
    endpoint = '%s/tasks?task_id=%s'%(mesos.endpoint, task_id)
    status, body, err = await self.http_cli.get(mesos.host, mesos.port, endpoint)
    if err:
      return status, None, err
    job['task'] = body['tasks'][0] if body['tasks'] else None
    return status, job, None

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
    await self.__contr_col.replace_one(dict(id=contr.id, appliance=contr.appliance),
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

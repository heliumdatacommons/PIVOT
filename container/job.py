from container.base import Container, NetworkMode, Resources, ContainerState

# Limitations:
# 1. Inefficient job state monitoring: Chronos does not have an API for per-job state
# 2. Cannot fetch job port mapping: Chronos API does not return either task ID or task
#                                   info associated with a job, so port mapping info
#                                   cannot be fetched, although it can be done through
#                                   Docker parameters
# 3. Cannot specify "resource role": as a consequence, jobs cannot be run on
#                                    "slave_public" hosts.

class Job(Container):

  def __init__(self, resources, network_mode=NetworkMode.HOST,
               retries=1, repeats=1, start_time='', interval='2M', **kwargs):
    super(Job, self).__init__(resources=resources, network_mode=network_mode, **kwargs)
    if self.resources.gpu > 0:
      raise ValueError('GPU is not yet supported for jobs')
    if self.network_mode == NetworkMode.CONTAINER:
      raise ValueError('CONTAINER mode is not supported for jobs')
    self.__retries = retries
    self.__repeats = repeats
    self.__start_time = start_time
    self.__interval = interval

  @property
  def interval(self):
    return self.__interval

  @property
  def retries(self):
    return self.__retries

  @property
  def repeats(self):
    return self.__repeats

  @property
  def start_time(self):
    return self.__start_time

  @classmethod
  def parse(cls, body, cluster):
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

  def to_render(self):
    return dict(**super(Job, self).to_render(),
                interval=self.interval, retries=self.retries,
                repeats=self.repeats, start_time=self.start_time)

  def to_save(self):
    return dict(**super(Job, self).to_save(),
                interval=self.interval, retries=self.retries,
                repeats=self.repeats, start_time=self.start_time)

  def to_request(self):
    r = dict(name=str(self),
             schedule='R%d/%s/P%s'%(self.repeats, self.start_time, self.interval),
             cpus=self.resources.cpus, mem=self.resources.mem, disk=self.resources.disk,
             shell=self.args is None,
             command = self.cmd if self.cmd else '',
             environmentVariables=[dict(name=k, value=v) for k, v in self.env.items()],
             container=dict(type='DOCKER',
                            image=self.image,
                            network=self.network_mode.value,
                            volumes=[v.to_request() for v in self.volumes],
                            forcePullImage=self.force_pull_image))
    if self.args:
      r['arguments'] = self.args
    parameters = [dict(key='privileged', value=self.is_privileged)]
    parameters += [dict(key='publish',
                        value='%d:%d/%s'%(p.host_port, p.container_port, p.protocol))
                   for p in self.ports]
    r['container']['parameters'] = parameters
    if self.rack:
      r.setdefault('constraints', []).append(['rack', 'EQUALS', self.rack])
    if self.host:
      r.setdefault('constraints', []).append(['hostname', 'EQUALS', self.host])
    return r

  def __str__(self):
    return '%s.%s'%(self.appliance, self.id)

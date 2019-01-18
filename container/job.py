import swagger

from container import Container, NetworkMode, parse_container_short_id
from volume import VolumeScope

# Limitations:
# 1. Inefficient job state monitoring: Chronos does not have an API for per-job state
# 2. Cannot fetch job port mapping: Chronos API does not return either task ID or task
#                                   info associated with a job, so port mapping info
#                                   cannot be fetched, although it can be done through
#                                   Docker parameters
# 3. Cannot specify "resource role": as a consequence, jobs cannot be run on
#                                    "slave_public" hosts.


@swagger.model
class Job(Container):
  """
  PIVOT Job

  """

  def __init__(self, resources, network_mode=NetworkMode.HOST,
               retries=1, repeats=1, start_time='', interval='2M', *args, **kwargs):
    super(Job, self).__init__(resources=resources, network_mode=network_mode, *args, **kwargs)
    if self.resources.gpu > 0:
      raise ValueError('GPU is not yet supported for jobs')
    self.__retries = retries
    self.__repeats = repeats
    self.__start_time = start_time
    self.__interval = interval

  @property
  @swagger.property
  def interval(self):
    """
    Interval between repetitions of the job
    ---
    type: str
    default: 2M
    example: 2M
    """
    return self.__interval

  @property
  @swagger.property
  def retries(self):
    """
    Maximum number of retries of the job on failures
    ---
    type: int
    default: 1
    example: 1

    """
    return self.__retries

  @property
  @swagger.property
  def repeats(self):
    """
    Number of repetitons of the job
    ---
    type: int
    default: 1
    example: 1

    """
    return self.__repeats

  @property
  @swagger.property
  def start_time(self):
    """
    Scheduled time to start the job. The job is started immediately if this field is left
    blank.
    ---
    type: datetime.datetime
    default: ''
    example: 2018-04-01T17:22:00Z
    """
    return self.__start_time

  def to_render(self):
    return dict(**super(Job, self).to_render(),
                interval=self.interval, retries=self.retries,
                repeats=self.repeats, start_time=self.start_time)

  def to_save(self):
    return dict(**super(Job, self).to_save(),
                interval=self.interval, retries=self.retries,
                repeats=self.repeats, start_time=self.start_time)

  def to_request(self):

    def get_default_env():
      return [dict(name='PIVOT_URL', value=parse_container_short_id('@pivot', 'sys'))]

    def get_default_parameters():
      return [dict(key='privileged', value=self.is_privileged),
              dict(key='rm', value='true'),
              #dict(key='oom-kill-disable', value='true')
              ]

    def get_port_mappings():
      return [dict(key='publish',
                   value='%d:%d/%s'%(p.host_port, p.container_port, p.protocol))
                   for p in self.ports]

    def get_persistent_volumes():
      params = []
      if len(self.persistent_volumes) == 0 \
          or isinstance(self.appliance, str) \
          or not self.appliance.data_persistence:
        return params
      params += [dict(key='volume-driver',
                      value=self.appliance.data_persistence.volume_type.driver)]
      params += [dict(key='volume',
                      value=('%s-%s:%s'%(self.appliance.id, v.src, v.dest)
                             if v.scope == VolumeScope.LOCAL else '%s:%s'%(v.src, v.dest)))
                 for v in self.persistent_volumes]
      return params

    params = get_default_parameters() \
             + get_port_mappings() \
             + get_persistent_volumes()
    r = dict(name=str(self),
             schedule='R%d/%s/P%s'%(self.repeats, self.start_time, self.interval),
             cpus=self.resources.cpus, mem=self.resources.mem, disk=self.resources.disk,
             shell=bool(self.cmd),
             command = self.cmd if self.cmd else '',
             retries=self.retries,
             environmentVariables=[dict(name=k,
                                        value=parse_container_short_id(v, self.appliance))
                                   for k, v in self.env.items()] + get_default_env(),
             container=dict(type='DOCKER',
                            image=self.image,
                            parameters=params,
                            network=self.network_mode.value,
                            volumes=[v.to_request() for v in self.host_volumes],
                            forcePullImage=self.force_pull_image))
    if self.args:
      r['arguments'] = [parse_container_short_id(a, self.appliance)
                        for a in self.args if str(a).strip()]
    if self.cmd:
      r['command'] = ' '.join([parse_container_short_id(p, self.appliance)
                               for p in self.cmd.split()])
    preemptible, placement = self.schedule_hints.preemptible, self.schedule_hints.placement
    r.setdefault('constraints', []).append(['preemptible', 'EQUALS', str(preemptible).lower()])
    if placement.host:
      r.setdefault('constraints', []).append(['hostname', 'EQUALS', str(placement.host)])
    elif placement.zone:
      r.setdefault('constraints', []).append(['zone', 'EQUALS', str(placement.zone)])
    elif placement.region:
      r.setdefault('constraints', []).append(['region', 'EQUALS', str(placement.region)])
    elif placement.cloud:
      r.setdefault('constraints', []).append(['cloud', 'EQUALS', str(placement.cloud)])
    return r

  def __repr__(self):
    return '%s.%s'%(self.appliance, self.id)

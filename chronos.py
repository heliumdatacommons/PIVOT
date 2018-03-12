class ChronosRequest:

  def __init__(self, contr):
    self.__contr = contr

  def to_render(self):
    contr = self.__contr
    params, env = [], []

    # check privileged mode
    if contr.is_privileged:
      params.append(dict(key='privileged', value=True))
    # add environment variables
    for k, v in contr.env.items():
      env.append(dict(name=k, value=v))
    volumes = [dict(containerPath=v.container_path, hostPath=v.host_path, mode=v.mode)
               for v in contr.volumes]
    chronos_req = dict(name=str(contr),
                       schedule='R1//P1Y',
                       shell=contr.args is None or contr.args == '',
                       cpus=contr.resources.cpus,
                       mem=contr.resources.mem,
                       disk=contr.resources.disk,
                       command=contr.cmd if contr.cmd else '',
                       arguments=contr.args,
                       environmentVariables=env,
                       container=dict(type='DOCKER',
                                      image=contr.image,
                                      network='BRIDGE',
                                      volumes=volumes,
                                      parameters=params,
                                      forcePullImage=contr.force_pull_image))
    if contr.host:
      chronos_req.setdefault('constraints', []).append(['hostname', 'EQUALS', contr.host])
    return chronos_req

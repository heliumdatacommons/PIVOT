
class MarathonRequest:

  def __init__(self, contr, network):
    self.__contr = contr
    self.__network = network

  def to_render(self):
    contr = self.__contr
    port_mappings = [dict(containerPort=p.container_port, hostPort=p.host_port,
                          protocol=p.protocol) for p in contr.port_mappings]
    volumes = [dict(containerPath=v.container_path, hostPath=v.host_path, mode=v.mode)
               for v in contr.volumes]
    params = self._get_host_parameters()
    return dict(id=str(contr),
                cpus=contr.resources.cpus,
                mem=contr.resources.mem,
                disk=contr.resources.disk,
                cmd=contr.cmd,
                args=contr.args,
                env=contr.env,
                container=dict(type='DOCKER',
                               volumes=volumes,
                               docker=dict(network='BRIDGE',
                                           image=contr.image,
                                           parameters=params,
                                           forcePullImage=contr.force_pull_image,
                                           portMappings=port_mappings,
                                           privileged=contr.is_privileged)),
                constraints = [['hostname', 'CLUSTER', contr.host]] if contr.host else [])

  def _get_host_parameters(self):
    if not self.__network:
      return None
    return [dict(key='add-host', value='%s:%s'%(n['id'], n['ip_addr']))
            for n in self.__network.containers]

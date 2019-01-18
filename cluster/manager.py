import datetime as dt


from config import config
from cluster import Master, Agent, AgentResources
from commons import AutonomousMonitor
from commons import APIManager, Manager


class ClusterManager(Manager):

  def __init__(self, monitor_interval=30000):
    self.__cluster_api = ClusterAPIManager()
    self.__cluster_monitor = ClusterMonitor(monitor_interval)

  async def update(self):
    await self.__cluster_monitor.update()

  async def get_agents(self, ttl=30):
    cluster_mon = self.__cluster_monitor
    if self._is_cache_expired(ttl):
      await cluster_mon.update()
    return list(cluster_mon.agents.values())

  async def get_agent(self, agent_id, ttl=30):
    assert isinstance(agent_id, str)
    cluster_mon = self.__cluster_monitor
    if self._is_cache_expired(ttl):
      await cluster_mon.update()
    return cluster_mon.agents.get(agent_id)

  def start_monitor(self):
    self.__cluster_monitor.start()

  def _is_cache_expired(self, ttl):
    if ttl is None:
      return False
    if not self.__cluster_monitor.last_update:
      return True
    ttl = dt.timedelta(seconds=ttl)
    return dt.datetime.now(tz=None) - self.__cluster_monitor.last_update > ttl


class ClusterMonitor(AutonomousMonitor):
  
  def __init__(self, interval=30000):
    super(ClusterMonitor, self).__init__(interval)
    self.__api = ClusterAPIManager()
    self._update_config(config.pivot.master)
    self.__agents = {}
    self.__last_update = None

  @property
  def agents(self):
    return dict(self.__agents)

  @property
  def last_update(self):
    return self.__last_update

  async def update(self):
    status, agents, err = await self.__api.get_agents()
    if status == 200:
      self.__agents.update({a.id: a for a in agents})
      self.__last_update = dt.datetime.now()
    else:
      self.logger.error('Failed to query Mesos agents: %s'%err)

  async def callback(self):
    await self.update()

  async def _discover_leader(self):
    if not config.pivot.master:
      config.pivot.master = 'zk-1.zk'
    config.exhibitor.host = config.pivot.master
    status, leader, err = await self.__api.get_leader()
    if status == 200:
      return leader
    raise Exception('Cannot find leader. Probably all the registered masters are down')

  def _update_config(self, host):
    config.pivot.master = host
    config.exhibitor.host = host
    config.mesos.host = host
    config.marathon.host = host
    config.chronos.host = host


class ClusterAPIManager(APIManager):

  async def get_masters(self):
    api = config.exhibitor
    status, masters, err = await self.http_cli.get(api.host, api.port,
                                                   '%s/cluster/status'%api.endpoint)
    if status != 200:
      self.logger.error('Failed to get cluster status: %s'%err)
      return status, None, err
    return status, [Master(m['hostname'], m['isLeader']) for m in masters], err

  async def get_leader(self):
    status, masters, err = await self.get_masters()
    if status != 200:
      return status, None, err
    leaders = [m for m in masters if m.is_leader]
    return status, leaders and leaders[0], err

  async def get_agents(self):
    api = config.mesos
    status, body, err = await self.http_cli.get(api.host, api.port,
                                                '%s/master/slaves'%api.endpoint)
    if status != 200:
      return status, None, err

    def calc_available_resource(h, type):
      if type == 'ports':
        ports, used = [list(h[t][type][1: -1].split(',')) if type in h[t] else []
                       for t in ('resources', 'used_resources')]
        ports = sorted([list(map(int, p.split('-'))) for p in ports])
        used = sorted([list(map(int, p.split('-'))) for p in used])
        unused, u_idx = [], 0
        for i, (ps, pe) in enumerate(ports):
          if u_idx == len(used):
            unused += ports[i + 1: ]
            break
          us, ue = used[u_idx]
          if pe < us:
            unused += (ps, pe),
          else:
            if ps < us:
              unused += (ps, us - 1),
            if pe > ue:
              unused += (ue + 1, pe),
            u_idx += 1
        return list(map(lambda x: '-'.join([str(i) for i in x]), unused))
      else:
        return(h['resources'].get(type, 0)
               - h['used_resources'].get(type, 0)
               - h['offered_resources'].get(type, 0)
               - h['reserved_resources'].get(type, 0))
    agents = [Agent(id=h['id'],
                    hostname=h['hostname'],
                    resources=AgentResources(calc_available_resource(h, 'cpus'),
                                             calc_available_resource(h, 'mem'),
                                             calc_available_resource(h, 'disk'),
                                             calc_available_resource(h, 'gpus'),
                                             calc_available_resource(h, 'ports')),
                    public_ip=h['attributes']['public_ip'],
                    cloud=h['attributes']['cloud'],
                    region=h['attributes']['region'],
                    zone=h['attributes']['zone'],
                    preemptible=bool(h['attributes']['preemptible']))
             for h in body['slaves']]
    return status, agents, None

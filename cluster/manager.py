import datetime

from config import config
from cluster.base import Master, Agent, AgentResources
from commons import MotorClient, AutonomousMonitor
from commons import APIManager, Manager


class ClusterManager(Manager):

  def __init__(self, monitor_interval=30000):
    cluster_api, agent_db = ClusterAPIManager(), AgentDBManager()
    self.__cluster_api, self.__agent_db = cluster_api, agent_db
    self.__cluster_monitor = ClusterMonitor(monitor_interval)

  async def get_cluster(self, ttl=30):
    if self._is_cache_expired(ttl):
      await self.__cluster_monitor.update()
    return await self.__agent_db.get_all_agents()

  async def find_agents(self, ttl=30, **kwargs):
    if self._is_cache_expired(ttl):
      await self.__cluster_monitor.update()
    return await self.__agent_db.find_agents(**kwargs)

  def start_monitor(self):
    self.__cluster_monitor.start()

  def _is_cache_expired(self, ttl):
    if ttl is None:
      return False
    if not self.__cluster_monitor.last_update:
      return True
    ttl = datetime.timedelta(seconds=ttl)
    return self.__cluster_monitor.last_update - datetime.datetime.now(tz=None) > ttl


class ClusterMonitor(AutonomousMonitor):
  
  def __init__(self, interval=30000):
    super(ClusterMonitor, self).__init__(interval)
    self.__api = ClusterAPIManager()
    self.__master_db = MasterDBManager()
    self.__agent_db = AgentDBManager()
    self.__last_update = None

  @property
  def last_update(self):
    return self.__last_update

  async def update(self):
    leader = await self._discover_leader()
    if leader.hostname != config.exhibitor.host:
      self.logger.info('Current leader: %s'%leader.hostname)
      self._update_config(leader.hostname)
    await self._discover_chronos()
    status, masters, err = await self.__api.get_masters()
    if status == 200:
      for m in masters:
        await self.__master_db.update_master(m)
    else:
      self.logger.error('Failed to query masters')
    status, agents, err = await self.__api.get_agents()
    if status == 200:
      for a in agents:
        await self.__agent_db.update_agent(a)
    else:
      self.logger.info('Failed to query agents')
    self.__last_update = datetime.datetime.now(tz=None)

  async def callback(self):
    await self.update()

  async def _discover_leader(self):
    if not config.pivot.master:
      config.pivot.master = 'zk-1.zk'
    config.exhibitor.host = config.pivot.master
    status, leader, err = await self.__api.get_leader()
    if status == 200:
      return leader
    for m in await self.__master_db.get_masters():
      config.exhibitor.host = m.hostname
      status, leader, err = await self.__api.get_leader()
      if status == 200:
        return leader
    raise Exception('Cannot find leader. Probably all the registered masters are down')

  async def _discover_chronos(self):
    status, body, err = await self.__api.find_chronos()
    if status != 200:
      self.logger.error(err)
      return
    config.chronos.host, config.chronos.port = body

  def _update_config(self, host):
    config.pivot.master = host
    config.exhibitor.host = host
    config.mesos.host = host
    config.marathon.host = host


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
    agents = [Agent(hostname=h['hostname'],
                   resources=AgentResources(h['resources']['cpus'],
                                            h['resources']['mem'],
                                            h['resources']['disk'],
                                            h['resources']['gpus'],
                                          h['resources']['ports'][1:-1].split(',')),
                   attributes=h['attributes'])
             for h in body['slaves']]
    return status, agents, None

  async def find_chronos(self):
    api = config.marathon
    status, body, err = await self.http_cli.get(api.host, api.port,
                                                '%s/apps/sys/chronos'%api.endpoint)
    if status != 200:
      return status, None ,err
    tasks = body['app']['tasks']
    if not tasks or not tasks[0].get('ports', []):
      return 503, None, 'Chronos is not yet ready'
    return 200, (tasks[0]['host'], tasks[0]['ports'][0]), None


class MasterDBManager(Manager):

  def __init__(self):
    self.__master_col = MotorClient().requester.master

  async def get_masters(self):
    return [Master(**m) async for m in self.__master_col.find()]

  async def get_leader_master(self):
    leaders = [Master(**m) async for m in self.__master_col.find(is_leader=True)]
    return leaders and leaders[0]

  async def update_master(self, master):
    await self.__master_col.replace_one(dict(hostname=master.hostname),
                                        master.to_save(), upsert=True)


class AgentDBManager(Manager):

  def __init__(self):
    self.__agent_col = MotorClient().requester.agent

  async def get_all_agents(self):
    return [Agent(**a, resources=AgentResources(**a.pop('resources', None)))
            async for a in self.__agent_col.find()]

  async def find_agents(self, **kwargs):
    cond = {'attributes.%s'%k: {'$in': v} if isinstance(v, list) else v
            for k, v in kwargs.items()}
    return [Agent(**a, resources=AgentResources(**a.pop('resources', None)))
            async for a in self.__agent_col.find(cond)]

  async def update_agent(self, agent):
    await self.__agent_col.replace_one(dict(hostname=agent.hostname), agent.to_save(),
                                       upsert=True)


import os
import yaml
import numpy as np
import numpy.linalg as la

from collections import defaultdict, Counter, deque

from locality import Placement
from schedule.universal import GlobalSchedulerBase


class CostAwareGlobalScheduler(GlobalSchedulerBase):

  def __init__(self):
    self.__locations = self._load_locality_data()
    self.__wait_q = deque()

  def schedule(self, tasks, volumes, agents, ensembles):
    wait_q = self.__wait_q
    resc = {a.id: np.array([a.resources.cpus, a.resources.mem, a.resources.disk, a.resources.gpus])
            for a in agents}
    tasks = deque(wait_q) + tasks
    if len(tasks) == 0:
      return [], volumes
    for loc, task_group in self._group_tasks(tasks, ensembles):
      # find anchor
      if isinstance(loc, Placement):
        qualified_agents = [a for a in agents
                            if a.locality.cloud == loc.cloud
                            and a.locality.region == loc.region and a.locality == loc.zone]
      else:
        qualified_agents = agents
      anchor = np.random.choice(qualified_agents)
      # sort tasks
      sorted_task_group = sorted(task_group,
                                 key=lambda t: la.norm([t.resources.cpus, t.resources.mem,
                                                        t.resources.disk, t.resources.gpus], 2))
      ready, wait = self._first_fit(agents, sorted_task_group, anchor, resc)
      wait_q += wait
      return ready, volumes

  def _load_locality_data(self):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    locations = {}
    with open(os.path.join(root_dir, 'locality.yml')) as f:
      locality_f = yaml.load(f)
      locality, meta = locality_f['locality'], locality_f['meta']
      for cloud, regions in locality.items():
        for region, zones in regions.items():
          for zone in zones:
            zone_name = '%s%s'%(region, zone) if cloud == 'aws' else '%s-%s'%(region, zone)
            locations.setdefault(Placement(cloud, region, zone_name), {})
      for key, vals in meta.items():
        src, dst = key.split('--')
        s_cloud, s_region = src.split('_')
        d_cloud, d_region = dst.split('_')
        for s_zone in locality[s_cloud][s_region]:
          for d_zone in locality[d_cloud][d_region]:
            s_zone_name = '%s%s'%(s_region, s_zone) if s_cloud == 'aws' else '%s-%s'%(s_region, s_zone)
            d_zone_name = '%s%s'%(d_region, d_zone) if d_cloud == 'aws' else '%s-%s'%(d_region, d_zone)
            sp, dp = Placement(s_cloud, s_region, s_zone_name), Placement(d_cloud, d_region, d_zone_name)
            locations[sp][dp] = dict(cost=vals['cost'], bw=vals['bw'])
    return locations

  def _group_tasks(self, tasks, ensembles):
    anchors = defaultdict(list)
    for t in tasks:
      app_id = t.appliance.id
      ensemble = ensembles[app_id]
      preds = ensemble.get_predecessors(t.id)
      if not preds:
        anchors[app_id] += t,
        continue
      loc = max(Counter([p.placement for p in preds]).items(), key=lambda x: x[1])
      anchors[loc] += t,
    return anchors.items()

  def _first_fit(self, agents, task_group, anchor, resc):
    locations = self.__locations

    def agent_score_func(a):
      r = la.norm(resc[a.id], 2)
      anchor_p, agent_p = anchor.locality.clone(), a.locality.clone()
      anchor_p.host = None
      agent_p.host = None
      bw = locations[anchor_p][agent_p]['bw'] + locations[agent_p][anchor_p]['bw']
      cost = locations[anchor_p][agent_p]['cost'] + locations[agent_p][anchor_p]['cost']
      return cost/(r * bw)

    agents = sorted(agents, key=agent_score_func)
    ready, wait = [], []
    for t in task_group:
      for a in agents:
        tr, ar = t.resources, resc[a.id]
        demand = np.array([tr.cpus, tr.mem, tr.disk, tr.gpus])
        available = ar
        if all(demand <= available):
          t.place(a)
          available -= demand
          break
      if t.schedule_hints.placement is None:
        wait += t,
      else:
        ready += t,
    return ready, wait



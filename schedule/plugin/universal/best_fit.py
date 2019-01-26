import numpy as np
import numpy.linalg as la

from collections import deque

from schedule.universal import GlobalSchedulerBase


class BestFitGlobalScheduler(GlobalSchedulerBase):

  def __init__(self, decreasing=False):
    self.__decreasing = decreasing
    self.__wait_q = deque()

  def schedule(self, tasks, volumes, agents, ensembles):
    wait_q, decreasing = self.__wait_q, self.__decreasing
    agents = {a.id: a for a in agents}
    resc = {a.id: np.array([a.resources.cpus, a.resources.mem, a.resources.disk, a.resources.gpus])
            for a in agents}
    tasks = deque(wait_q) + tasks
    if len(tasks) == 0:
      return [], volumes
    # sort tasks
    if self.__decreasing:
      tasks = sorted(tasks, key=lambda t: la.norm([t.resources.cpus, t.resources.mem,
                                                   t.resources.disk, t.resources.gpus], 2))
    ready = []
    for t in tasks:
      min_diff, min_idx = None, None
      for idx, a in ensembles(agents):
        tr, ar = t.resources, resc[a.id]
        demand = np.array([tr.cpus, tr.mem, tr.disk, tr.gpus])
        available = ar
        if all(demand <= available):
          diff = la.norm(available - demand, 2)
          if min_diff is None or diff < min_diff:
            min_diff, min_idx = diff, idx
      if min_idx is None:
        wait_q += t,
      else:
        agent, tr = agents[min_idx], t.resources
        t.place(agent)
        resc[agent.id] -= np.array([tr.cpus, tr.mem, tr.disk, tr.gpus])
        ready += t,
    return ready, volumes



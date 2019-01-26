import numpy as np
import numpy.linalg as la

from collections import deque

from schedule.universal import GlobalSchedulerBase


class FirstFitGlobalScheduler(GlobalSchedulerBase):

  def __init__(self, decreasing=False):
    self.__decreasing = decreasing
    self.__wait_q = deque()

  def schedule(self, tasks, volumes, agents, ensembles):
    wait_q, decreasing = self.__wait_q, self.__decreasing
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
      for a in agents:
        tr, ar = t.resources, resc[a.id]
        demand = np.array([tr.cpus, tr.mem, tr.disk, tr.gpus])
        available = ar
        if all(demand <= available):
          t.place(a)
          available -= demand
          break
      if t.schedule_hints.placement is None:
        wait_q += t,
      else:
        ready += t,
    return ready, volumes



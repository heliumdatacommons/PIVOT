import networkx as nx
import datetime as dt

from enum import Enum
from collections import Iterable

from locality import Placement
from commons import Loggable


class TaskState(Enum):

  TASK_SUBMITTED = 'TASK_SUBMITTED'
  TASK_STAGING = 'TASK_STAGING'
  TASK_STARTING = 'TASK_STARTING'
  TASK_RUNNING = 'TASK_RUNNING'
  TASK_FINISHED = 'TASK_FINISHED'
  TASK_FAILED = 'TASK_FAILED'
  TASK_KILLED = 'TASK_KILLED'
  TASK_KILLING = 'TASK_KILLING'
  TASK_LOST = 'TASK_LOST'
  TASK_ERROR = 'TASK_ERROR'
  TASK_DROPPED = 'TASK_DROPPED'
  TASK_UNREACHABLE = 'TASK_UNREACHABLE'
  TASK_UNKNOWN = 'TASK_UNKNOWN'
  TASK_GONE = 'TASK_GONE'


class Task(Loggable):

  MAX_LAUNCH_DELAY = 60
  
  def __init__(self, container, seqno, launch_time=None, dependencies=[], schedule_hints=None, *args, **kwargs):
    from container import Container
    from schedule import ScheduleHints
    assert isinstance(container, Container)
    self.__container = container
    assert isinstance(seqno, int)
    self.__seqno =seqno
    assert launch_time is None or isinstance(launch_time, dt.datetime)
    self.__launch_time = launch_time
    assert isinstance(dependencies, Iterable) and all([isinstance(d, Task) for d in dependencies])
    self.__dependencies = dependencies
    assert schedule_hints is None or isinstance(schedule_hints, ScheduleHints)
    self.__schedule_hints = schedule_hints
    self.__endpoints = []
    self.__state = None
    self.__placement = None
    self.__mesos_task_id = None

  @property
  def id(self):
    return '%s-%d'%(self.container.id, self.seqno)

  @property
  def seqno(self):
    return self.__seqno

  @property
  def container(self):
    return self.__container

  @property
  def launch_time(self):
    return self.__launch_time

  @property
  def dependencies(self):
    return list(self.__dependencies)

  @property
  def schedule_hints(self):
    return self.__schedule_hints

  @property
  def endpoints(self):
    return list(self.__endpoints)

  @property
  def state(self):
    return self.__state

  @property
  def placement(self):
    return self.__placement

  @property
  def mesos_task_id(self):
    return self.__mesos_task_id

  @property
  def launch_delay(self):
    return (dt.datetime.now() - self.launch_time).total_seconds() if self.launch_time else 0

  @launch_time.setter
  def launch_time(self, lt):
    assert isinstance(lt, dt.datetime)
    self.__launch_time = lt

  @schedule_hints.setter
  def schedule_hints(self, hints):
    from schedule import ScheduleHints
    assert isinstance(hints, ScheduleHints)
    self.__schedule_hints = hints

  @state.setter
  def state(self, state):
    assert isinstance(state, str) or isinstance(state, TaskState)
    self.__state = state if isinstance(state, TaskState) else TaskState(state.upper())

  @placement.setter
  def placement(self, placement):
    assert isinstance(placement, Placement)
    self.__placement = placement

  @mesos_task_id.setter
  def mesos_task_id(self, mesos_task_id):
    assert mesos_task_id is None or isinstance(mesos_task_id, str)
    self.__mesos_task_id = mesos_task_id

  def add_dependencies(self, *deps):
    assert all([isinstance(d, Task) for d in deps])
    self.__dependencies = list(set(self.__dependencies + [d.id for d in deps]))

  def add_endpoints(self, *endpoints):
    from container import Endpoint
    assert all([isinstance(e, Endpoint) for e in endpoints])
    self.__endpoints = list(set(self.__endpoints + list(endpoints)))

  def to_render(self):
    return dict(id=self.id,
                mesos_task_id=self.mesos_task_id,
                launch_time=self.launch_time)

  def to_save(self):
    return dict(seqno=self.seqno,
                mesos_task_id=self.mesos_task_id,
                launch_time=self.launch_time)

  def __hash__(self):
    return hash((self.container, self.seqno))

  def __eq__(self, other):
    return isinstance(other, Task) \
           and self.container == other.container \
           and self.seqno == other.seqno

  def __repr__(self):
    return self.id
  

class ServiceTask(Task):
  
  def __init__(self, service, *args, **kwargs):
    from container.service import Service
    assert isinstance(service, Service)
    super(ServiceTask, self).__init__(service, *args, **kwargs)


class JobTask(Task):
  
  def __init__(self, job, *args, **kwargs):
    from container.job import Job
    assert isinstance(job, Job)
    super(JobTask, self).__init__(job, *args, **kwargs)


class TaskEnsemble(Loggable):

  def __init__(self, appliance):
    from appliance import Appliance
    assert isinstance(appliance, Appliance)
    self.__appliance = appliance
    self.__tasks = {}
    self.__tasks_by_contr = {}
    self.__task_idx = {}
    self.__task_reverse_idx = {}
    self.__dag = self._create_dag()
    self.__cur_tasks = None

  @property
  def appliance(self):
    return self.__appliance

  @property
  def tasks(self):
    return list(self.__tasks.values())

  @property
  def is_finished(self):
    return all([s == TaskState.TASK_FINISHED for s in self.get_sinks()])

  @property
  def current_tasks(self):
    return list(self.__cur_tasks) if self.__cur_tasks else []

  @property
  def ready_tasks(self):
    cur_tasks = self.__cur_tasks
    if cur_tasks is None:
      self.__cur_tasks = self.get_sources()
      return list(self.__cur_tasks)
    self.logger.debug("Current tasks: %d"%len(cur_tasks))
    ready_tasks, new_cur_tasks = [], []
    for t in cur_tasks:
      self.logger.debug("%s: %s, delay: %.2f"%(t, t.state.value, t.launch_delay))
    while cur_tasks:
      t = cur_tasks.pop()
      if t.state == TaskState.TASK_STAGING \
          or t.state == TaskState.TASK_STARTING \
          or (isinstance(t, JobTask) and t.state == TaskState.TASK_RUNNING):
        # if the task is in the staging/starting state or it is a running job task
        new_cur_tasks += t,
      elif t.state == TaskState.TASK_SUBMITTED:
        if t.launch_delay > Task.MAX_LAUNCH_DELAY:
          # if the task stays in the submitted state for too long time
          self.logger.debug('Task [%s] does not start in %d seconds, '
                            'ready for relaunch'%(t, Task.MAX_LAUNCH_DELAY))
          ready_tasks += t,
        new_cur_tasks += t,
      elif (isinstance(t, ServiceTask) and t.state == TaskState.TASK_RUNNING) \
          or (isinstance(t, JobTask) and t.state == TaskState.TASK_FINISHED):
        # if a service task is running or a job task is finished
        succs = self.get_ready_successors(t.id)
        new_cur_tasks += succs
        ready_tasks += succs
      elif t.state not in (TaskState.TASK_STAGING, TaskState.TASK_STARTING, TaskState.TASK_RUNNING,
                           TaskState.TASK_FINISHED, TaskState.TASK_SUBMITTED):
        self.logger.debug('Task [%s] is in a problematic state: %s, ready for relaunch'%(t, t.state.value))
        # if the task is in a problematic state
        t.state, t.mesos_task_id = TaskState.TASK_SUBMITTED, None
        new_cur_tasks += t,
        ready_tasks += t,
    self.__cur_tasks = new_cur_tasks
    return set(ready_tasks)

  def get_task_by_id(self, task_id):
    return self.__tasks.get(task_id)

  def get_tasks_by_container_id(self, contr_id):
    return list(self.__tasks_by_contr.get(contr_id, []))

  def get_finished_tasks(self, contr_id):
    return [t for t in self.get_tasks_by_container_id(contr_id)
            if t.state == TaskState.TASK_FINISHED]

  def get_running_tasks(self, contr_id):
    return [t for t in self.get_tasks_by_container_id(contr_id)
            if t.state == TaskState.TASK_RUNNING]

  def get_staging_tasks(self, contr_id):
    return [t for t in self.get_tasks_by_container_id(contr_id)
            if t.state == TaskState.TASK_STAGING]

  def get_submitted_tasks(self, contr_id):
    return [t for t in self.get_tasks_by_container_id(contr_id)
            if t.state == TaskState.TASK_SUBMITTED]

  def get_other_tasks(self, contr_id):
    return [t for t in self.get_tasks_by_container_id(contr_id)
            if t.state not in (TaskState.TASK_STAGING, TaskState.TASK_FINISHED,
                               TaskState.TASK_RUNNING, TaskState.TASK_SUBMITTED)]

  def get_sources(self):
    task_idx, dag = self.__task_idx, self.__dag
    return [task_idx[n] for n in dag.nodes if dag.in_degree(n) == 0]

  def get_sinks(self):
    task_idx, dag = self.__task_idx, self.__dag
    return [task_idx[n] for n in dag.nodes if dag.out_degree(n) == 0]

  def get_predecessors(self, task_id):
    dag, task_idx, task_reverse_idx = self.__dag, self.__task_idx, self.__task_reverse_idx
    idx = task_reverse_idx.get(task_id)
    return [task_idx[p] for p in dag.predecessors(idx)] if idx else []

  def get_successors(self, task_id):
    dag, task_idx, task_reverse_idx = self.__dag, self.__task_idx, self.__task_reverse_idx
    idx = task_reverse_idx.get(task_id)
    return [task_idx[s] for s in dag.successors(idx)] if idx else []

  def get_unready_predecessor(self, task_id):
    return [p for p in self.get_predecessors(task_id)
            if (isinstance(p, ServiceTask) and p.state != TaskState.TASK_RUNNING)
            or (isinstance(p, JobTask) and p.state != TaskState.TASK_FINISHED)]

  def get_ready_successors(self, task_id):
    return [s for s in self.get_successors(task_id)
            if len(self.get_unready_predecessor(s.id)) == 0]

  def _create_dag(self):
    from container.service import Service
    app = self.__appliance
    task_idx, task_reverse_idx = self.__task_idx, self.__task_reverse_idx
    dag = nx.DiGraph()
    tasks, tasks_by_contr, counter = self.__tasks, self.__tasks_by_contr, 0
    for c in app.containers:
      tasks_by_contr[c.id] = [ServiceTask(c, i) if isinstance(c, Service) else JobTask(c, i)
                              for i in range(c.instances)]
      c.add_tasks(*tasks_by_contr[c.id])
      new_tasks = {counter + i: t for i, t in enumerate(tasks_by_contr[c.id])}
      task_idx.update(new_tasks)
      task_reverse_idx.update({t.id: i for i, t in new_tasks.items()})
      counter += len(new_tasks)

    tasks.update({t.id: t for t in task_idx.values()})
    for i in task_idx:
      dag.add_node(i)

    for c in app.containers:
      for d in c.dependencies:
        src_tasks, dst_tasks = tasks_by_contr[d], tasks_by_contr[c.id]
        for dst in dst_tasks:
          if len(src_tasks) > 0:
            dst.add_dependencies(*src_tasks)
          for src in src_tasks:
            dag.add_edge(task_reverse_idx[src.id], task_reverse_idx[dst.id])
    if not nx.is_directed_acyclic_graph(dag):
      raise ValueError('Container(s) in the appliance cannot create a DAG')
    return dag
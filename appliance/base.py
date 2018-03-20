from collections import defaultdict
from container.base import Container


class Appliance:

  REQUIRED = frozenset(['id', 'containers'])

  def __init__(self, id, containers=[], **kwargs):
    self.__id = id
    self.__containers = list(containers)
    self.__dag = ContainerDAG()

  @classmethod
  def pre_check(cls, data):
    if not isinstance(data, dict):
      return 422, None, "Failed to parse appliance request format: %s"%type(data)
    missing = Appliance.REQUIRED - data.keys()
    if missing:
      return 400, None, "Missing required field(s) of appliance: %s"%missing
    for c in data['containers']:
      status, _, err = Container.pre_check(c)
      if err:
        return status, None, err
    return 200, "Appliance %s is valid" % data['id'], None

  @property
  def id(self):
    return self.__id

  @property
  def containers(self):
    return self.__containers

  @property
  def dag(self):
    return self.__dag

  @containers.setter
  def containers(self, contrs):
    self.__containers = list(contrs)

  def to_render(self):
    return dict(id=self.id,
                containers=[c.to_render() for c in self.containers])

  def to_save(self):
    return dict(id=self.id, dag=self.dag.to_save())

  def __str__(self):
    return self.id


class ContainerDAG:

  def __init__(self):
    self.__containers = {}
    self.__parent_map, self.__child_map = None, None

  @property
  def parent_map(self):
    return {k: list(v) for k, v in self.__parent_map.items()}

  @property
  def child_map(self):
    return {k: list(v) for k, v in self.__child_map.items()}

  @property
  def is_empty(self):
    return len(self.__parent_map) == 0

  def get_free_containers(self):
    return [self.__containers[k] for k, v in self.__parent_map.items() if not v]

  def update_container(self, contr):
    self.__containers[contr.id] = contr

  def remove_container(self, contr_id):
    for child in self.__child_map.pop(contr_id, set()):
      self.__parent_map.get(child, set()).remove(contr_id)
    self.__parent_map.pop(contr_id, None)

  def construct_graph(self, contrs):
    assert all(isinstance(c, Container) for c in contrs)
    parent_map, child_map = defaultdict(set), defaultdict(set)
    containers = {c.id: c for c in contrs}
    # check dependency validity
    for c in contrs:
      nonexist_deps = list(filter(lambda c: c not in containers, c.dependencies))
      if nonexist_deps:
        return 422, None, "Dependencies '%s' do not exist in this appliance"%nonexist_deps
      parent_map[c.id].update(c.dependencies)
      for d in c.dependencies:
        child_map[d].add(c.id)
    # check cycles
    for c, parents in parent_map.items():
      cyclic_deps = ['%s<->%s'%(c, p)
                     for p in filter(lambda p: c in parent_map[p], parents)]
      if cyclic_deps:
        return 422, None, "Cycle(s) found: %s"%cyclic_deps
    self.__containers = containers
    self.__parent_map, self.__child_map = parent_map, child_map
    return 200, "DAG constructed successfully", None

  def restore(self, dag, contrs):
    assert isinstance(dag, dict)
    assert all(isinstance(c, Container) for c in contrs)
    self.__parent_map = dag.get('parent_map', {})
    self.__child_map = dag.get('child_map', {})
    self.__containers = {c.id: c for c in contrs if c.id not in self.__parent_map}

  def to_save(self):
    return dict(parent_map=self.parent_map, child_map=self.child_map)







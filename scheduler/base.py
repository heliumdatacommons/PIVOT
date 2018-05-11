class ApplianceDAG:

  @classmethod
  def construct_dag(cls, app):
    dag = ApplianceDAG(app)
    status, msg, err = dag.construct()
    if status != 200:
      return status, msg, err
    return status, dag, None

  def __init__(self, app, parent_map={}, child_map={}, *args, **kwargs):
    self.__app = app
    self.__containers = {c.id: c for c in app.containers}
    self.__parent_map = {k: set(v) for k, v in parent_map.items()}
    self.__child_map = {k: set(v) for k, v in child_map.items()}

  @property
  def appliance(self):
    return self.__app.id

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
      self.__parent_map.setdefault(child, set()).remove(contr_id)
    self.__parent_map.pop(contr_id, None)

  def to_save(self):
    return dict(appliance=self.appliance,
                parent_map=self.parent_map, child_map=self.child_map)

  def construct(self):
    parent_map, child_map = self.__parent_map, self.__child_map
    if not parent_map or not child_map:
      # check dependency validity
      for c in self.__containers.values():
        nonexist_deps = list(filter(lambda c: c not in self.__containers, c.dependencies))
        if nonexist_deps:
          return 422, None, "Dependencies '%s' do not exist in this appliance"%nonexist_deps
        parent_map.setdefault(c.id, set()).update(c.dependencies)
        for d in c.dependencies:
          child_map.setdefault(d, set()).add(c.id)
      # check cycles
      for c, parents in parent_map.items():
        cyclic_deps = ['%s<->%s'%(c, p)
                       for p in filter(lambda p: c in parent_map.setdefault(p, set()),
                                       parents)]
        if cyclic_deps:
          return 422, None, "Cycle(s) found: %s"%cyclic_deps
    self.__parent_map, self.__child_map = parent_map, child_map
    return 200, "DAG constructed successfully", None

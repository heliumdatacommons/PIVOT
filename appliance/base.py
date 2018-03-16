from collections import deque


class Appliance:

  def __init__(self, id, containers=[], pending=[], **kwargs):
    self.__id = id
    self.__containers = list(containers)
    self.__pending = deque(pending)

  @property
  def id(self):
      return self.__id

  @property
  def containers(self):
    return self.__containers

  @property
  def pending(self):
    return list(self.__pending)

  @containers.setter
  def containers(self, contrs):
    self.__containers = list(contrs)

  def enqueue_pending_containers(self, contr_id):
    self.__pending.append(contr_id)

  def dequeue_pending_container(self):
    return self.__pending.popleft()

  def to_render(self):
    return dict(id=self.id,
                containers=[c if isinstance(c, str) else c.to_render()
                            for c in self.containers])

  def to_save(self):
    return dict(id=self.id,
                containers=[c if isinstance(c, str) else c.id for c in self.containers],
                pending=self.pending)

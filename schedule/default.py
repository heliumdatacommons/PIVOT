from container.base import ContainerType, ContainerState
from schedule import ApplianceScheduler, Schedule


class DefaultApplianceScheduler(ApplianceScheduler):

  async def schedule(self, app, agents):
    sched = Schedule()
    free_contrs = self.resolve_dependencies(app)
    self.logger.debug('Free containers: %s'%[c.id for c in free_contrs])
    if not free_contrs:
      sched.done = True
      return sched
    sched.add_containers([c for c in free_contrs if c.state in
                          (ContainerState.SUBMITTED, ContainerState.FAILED)])
    return sched

  def resolve_dependencies(self, app):
    contrs = {c.id: c for c in app.containers
              if (c.type == ContainerType.JOB and c.state != ContainerState.SUCCESS)
              or (c.type == ContainerType.SERVICE and c.state != ContainerState.RUNNING)}
    parents = {}
    for c in contrs.values():
      parents.setdefault(c.id, set()).update([d for d in c.dependencies if d in contrs])
    return [contrs[k] for k, v in parents.items() if not v]



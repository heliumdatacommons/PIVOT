from scheduler.base import ApplianceDAG
from appliance.base import Appliance
from container.manager import ContainerDBManager
from commons import Manager, MotorClient


class SchedulerManager(Manager):

  def __init__(self):
    self.__contr_db = ContainerDBManager()
    self.__dag_db = ApplianceDAGDBManager()

  async def get_appliance_dag(self, app_id):
    containers = await self.__contr_db.get_containers(appliance=app_id)
    if not containers:
      return 404, None, "No containers found for appliance '%s'"%app_id
    status, dag, err = await  self.__dag_db.get_appliance_dag(app_id)
    if status != 200:
      return status, None, err
    dag = ApplianceDAG(Appliance(app_id, containers), **dag)
    dag.construct()
    return 200, dag, None

  async def update_appliance_dag(self, dag, upsert=False):
    return await self.__dag_db.update_appliance_dag(dag, upsert)


class ApplianceDAGDBManager(Manager):

  def __init__(self):
    self.__dag_col = MotorClient().requester.dag

  async def get_appliance_dag(self, app_id):
    dag = await self.__dag_col.find_one(dict(appliance=app_id))
    if not dag:
      return 404, None, "DAG of appliance '%s' is not found"%app_id
    return 200, dag, None

  async def delete_appliance_dag(self, app_id):
    await self.__dag_col.delete_one(dict(appliance=app_id))
    return 200, "DAG of appliance '%s' is deleted"%app_id, None

  async def update_appliance_dag(self, dag, upsert=False):
    await self.__dag_col.replace_one(dict(appliance=dag.appliance), dag.to_save(),
                                     upsert=upsert)
    return 200, "DAG of appliance '%s' is updated"%dag.appliance, None




from util import Singleton, Loggable, MotorClient, SecureAsyncHttpClient
from appliance.base import Appliance
from container.manager import ContainerManager


class ApplianceManager(Loggable, metaclass=Singleton):

  def __init__(self, config):
    self.__config = config
    self.__app_col = MotorClient().requester.appliance
    self.__contr_mgr = ContainerManager(config)
    self.__http_cli = SecureAsyncHttpClient(config)

  async def get_appliance(self, app_id):
    status, app, err = await self._get_appliance_from_db(app_id)
    if status != 200:
      return status, app, err
    app = Appliance(**app)
    _, app.containers, _ = await self.__contr_mgr.get_containers(app_id)
    return 200, app, None

  async def create_appliance(self, app):
    pass

  async def delete_appliance(self, app_id):
    status, app, err = await self._get_appliance_from_db(app_id)
    if err:
      self.logger.error(err)
      return status, None, err
    status, msg, err = await self.__contr_mgr.delete_containers(appliance=app_id)
    if err:
      self.logger.error(err)
      return 400, None, "Failed to deprovision containers of appliance '%s'"%app_id
    self.logger.info(msg)
    status, msg, err = await self._deprovision_group(app_id)
    if err:
      self.logger.error(err)
      return 400, None, "Failed to deprovision group of appliance '%s'"%app_id
    self.logger.info(msg)
    status, msg, err = await self._delete_appliance_from_db(app_id)
    if err:
      self.logger.error(err)
      return status, None, err
    self.logger.info(msg)
    return status, msg, None

  async def save_appliance(self, app, upsert=True):
    await self.__app_col.replace_one(dict(id=app.id), app.to_save(), upsert=upsert)

  async def _get_appliance_from_db(self, app_id):
    app = await self.__app_col.find_one(dict(id=app_id))
    if not app:
      return 404, None, "Appliance '%s' is not found"%app_id
    return 200, app, None

  async def _delete_appliance_from_db(self, app_id):
    await self.__app_col.delete_one(dict(id=app_id))
    return 200, "Appliance '%s' has been deleted"%app_id, None

  async def _deprovision_group(self, app_id):
    url = '%s/groups/%s?force=true'%(self.__config.url.service_scheduler, app_id)
    status, msg, err = await self.__http_cli.delete(url)
    if err:
      return status, None, err
    return 200, "Services of appliance '%s' have been deprovisioned"%app_id, None


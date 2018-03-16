from util import Singleton, Loggable, MotorClient, SecureAsyncHttpClient
from container.manager import ContainerManager

class ApplianceManager(Loggable, metaclass=Singleton):

  def __init__(self):
    self.__app_col = MotorClient().requester.appliance
    self.__contr_mgr = ContainerManager()
    self.__http_cli = SecureAsyncHttpClient()

  async def get_appliance(self, app_id, verbose=True):
    pass


  async def create_appliance(self, app):
    pass

  async def delete_appliance(self, app_id):
    pass

  async def save_appliance(self, app, upsert=True):
    await self.__app_col.replace_one(dict(id=app.id), app.to_save(), upsert=upsert)

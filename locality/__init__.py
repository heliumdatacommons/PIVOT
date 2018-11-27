import swagger


@swagger.model
class Placement:
  """
  Placement of containers/volumes of an appliance

  """

  def __init__(self, cloud=None, region=None, zone=None, host=None):
    self.__cloud = cloud
    self.__region = region
    self.__zone = zone
    self.__host = host

  @property
  @swagger.property
  def cloud(self):
    """
    Cloud platform
    ---
    type: str
    read_only: true
    example: aws

    """
    return self.__cloud

  @property
  @swagger.property
  def region(self):
    """
    Geographical region
    ---
    type: str
    read_only: true
    example: us-east-1

    """
    return self.__region

  @property
  @swagger.property
  def zone(self):
    """
    Availability zone
    ---
    type: str
    read_only: true
    example: us-east-1a

    """
    return self.__zone

  @property
  @swagger.property
  def host(self):
    """
    Hostname
    ---
    type: str
    read_only: true
    example: 10.52.100.3

    """
    return self.__host

  @cloud.setter
  def cloud(self, cloud):
    self.__cloud = cloud

  @region.setter
  def region(self, region):
    self.__region = region

  @zone.setter
  def zone(self, zone):
    self.__zone = zone

  @host.setter
  def host(self, host):
    self.__host = host

  def to_render(self):
    return dict(cloud=self.cloud, region=self.region, zone=self.zone, host=self.host)

  def to_save(self):
    return self.to_render()




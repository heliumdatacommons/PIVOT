import swagger
import datetime


class Enum:

  def __init__(self, name, type, values=[]):
    self.__name = name
    self.__type = type
    self.__values = list(values)

  def to_dict(self):
    return {self.__name: dict(type=self.__type, enum=list(self.__values))}


class Model:

  def __init__(self, name, type='object', description='', properties=[], ref=None):
    self.__name = name
    self.__type = type
    self.__description = description
    self.__properties = list(properties)
    self.__ref = ref

  @property
  def name(self):
    return self.__name

  @property
  def description(self):
    return self.__description

  @property
  def type(self):
    return self.__type

  @description.setter
  def description(self, description):
    self.__description = description

  @type.setter
  def type(self, type):
    self.__type = type

  def add_property(self, p):
    assert isinstance(p, Property)
    self.__properties.append(p)

  def to_dict(self):
    res = dict(type='object')
    if self.__description:
      res.update(description=self.description)
    if self.__ref:
      res.update(ref=self.__ref)
    if self.__properties:
      res.update(required=[p.name for p in self.__properties if p.required],
                 properties={p.name: p.to_dict() for p in self.__properties})
    if self.type != 'object':
      res = {'allOf': [{'$ref': '#/components/schemas/%s'%self.type}, res]}
    return res


class Property:

  def __init__(self, name, **kwargs):
    self.__name = name
    self.update(**kwargs)


  @property
  def name(self):
    return self.__name

  @property
  def type(self):
    return self.__type

  @property
  def description(self):
    return self.__description

  @property
  def required(self):
    return self.__required

  @type.setter
  def type(self, type):
    self.__type = type and swagger._convert_data_type(type, self.__additional_properties)

  @description.setter
  def description(self, description):
    self.__description = description

  def update(self, type=None, description=None, items=None, required=False,
             nullable=False, additional_properties=None, default=None,
             read_only=False, write_only=False, example=None,
             maximum=None, minimum=None, **kwargs):
    assert not (read_only and write_only)
    self.__type = type and swagger._convert_data_type(type, additional_properties)
    self.__description = description
    self.__items = items and swagger._convert_data_type(items, additional_properties)
    self.__required = required
    self.__nullable = nullable
    self.__additional_properties = additional_properties
    self.__default = default
    self.__read_only = read_only
    self.__write_only = write_only
    self.__example = example
    self.__maximum = maximum
    self.__minimum = minimum

  def to_dict(self):
    res = {}
    if self.__type:
      res.update(**self.__type)
    if self.__description:
      res.update(description=self.__description)
    if self.__nullable:
      res.update(nullable=self.__nullable)
    if self.__items:
      res.update(items=self.__items)
    if self.__default is not None:
      res.update(default=self.__default)
    if self.__read_only:
      res.update(readOnly=self.__read_only)
    if self.__write_only:
      res.update(writeOnly=self.__write_only)
    if self.__example:
      res.update(example=self.__example.strftime("%Y-%m-%d %H:%M:%S")
                         if isinstance(self.__example, datetime.datetime)
                         else self.__example)
    if self.__maximum is not None:
      res.update(maximum=self.__maximum)
    if self.__minimum is not None:
      res.update(minimum=self.__minimum)

    return res

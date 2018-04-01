import yaml
import inspect
import datetime
import collections

from enum import Enum
from functools import wraps

from swagger.base import Model, Property
from util import Loggable, Singleton


DATA_TYPES = {
  str: dict(type='string'),
  # reference: https://docs.python.org/2.4/lib/typesnumeric.html
  int: dict(type='integer', format='int64'),
  float: dict(type='number', format='double'),
  bool: dict(type='boolean'),
  bytes: dict(type='string', format='byte'),
  datetime.date: dict(type='string', format='date'),
  datetime.datetime: dict(type='string', format='date-time'),
  list: dict(type='array'),
  tuple: dict(type='array'),
  set: dict(type='array')
}


def _ref(model):
  assert isinstance(model, str)
  return {'$ref': '#/components/schemas/%s'%model}


def _convert_data_type(data_type, additional_properties=None):
  assert isinstance(data_type, str)
  try:
    data_type = eval(data_type)
    if data_type in DATA_TYPES:
      return DATA_TYPES[data_type]
    if data_type in (dict, object):
      if additional_properties:
        additional_properties = {k: _convert_data_type(v)
                                 for k, v in additional_properties.items()}
      return dict(type='object',
                  additionalProperties=additional_properties or dict(type='object'))
  except:
    return _ref(data_type)


def _format_docstring(docstring):
  if not docstring:
    return docstring
  return docstring.strip().replace('\n', ' ')

def _get_class_name(cls):
  return cls.__name__.split('.')[-1]


class enum:

  def __new__(cls, *args, **kwargs):
    enum_cls = args[0]
    SwaggerAPIRegistry().register_enum(enum_cls)
    return enum_cls


class model:

  def __new__(cls, *args, **kwargs):
    model_cls = args[0]
    SwaggerAPIRegistry().register_model(model_cls)
    return model_cls


class operation:

  def __new__(self, *args, **kwargs):
    func = args[0]

    @wraps(func)
    def __wrapper__(*f_args, **f_kwargs):
      return func(*f_args, **f_kwargs)

    signature = inspect.signature(func)
    __wrapper__.func_args = list(signature.parameters.keys())[1:]
    return __wrapper__


class property:

  def __new__(cls, *args, **kwargs):
    prop = args[0]
    model, _ = prop.__qualname__.split('.')
    SwaggerAPIRegistry().register_property(model, prop)
    return prop


class SwaggerAPIRegistry(Loggable, metaclass=Singleton):

  def __init__(self):
    self.__operations = collections.defaultdict(list)
    self.__models = []
    self.__enums = []
    self.__properties = collections.defaultdict(list)
    self.__specs = None

  def register_operations(self, app):
    for r in app.wildcard_router.rules:
      for name, member in inspect.getmembers(r.target):
        if hasattr(member, 'func_args'):
          path = r.matcher._path%tuple('{%s}'%a for a in member.func_args)
          self.__operations[path].append(member)

  def register_enum(self, enum):
    self.__enums.append(enum)

  def register_model(self, model):
    self.__models.append(model)

  def register_property(self, model, property):
    self.__properties[model].append(property)

  def get_api_specs(self):
    if not self.__specs:
      self.__specs = self._generate_api_specs()
    return self.__specs

  def _generate_api_specs(self):
    spec = dict(
      openapi='3.0.0',
      info=dict(
        title='Helium DataCommons PIVOT',
        version='0.1',
        description=''
      ),
      components=dict(schemas={})
    )
    spec['components']['schemas'].update(self._parse_enums())
    spec['components']['schemas'].update(self._parse_models())

    return spec

  def _parse_enums(self):
    enums = {}
    for enum in self.__enums:
      values = [m.value for _, m in inspect.getmembers(enum) if isinstance(m, Enum)]
      if not values:
        continue
      enums[enum.__name__] = dict(**_convert_data_type(type(values[0]).__name__),
                                  description=_format_docstring(enum.__doc__),
                                  enum=values)
    return enums

  def _parse_models(self):
    models = {}
    for m_cls in self.__models:
      model = Model(m_cls.__name__, type=_get_class_name(m_cls.__bases__[0]),
                    description=_format_docstring(m_cls.__doc__))
      for p in self.__properties.get(model.name, []):
        property = Property(p.__name__)
        if p.__doc__:
          description, doc = p.__doc__.split('---')
          property.update(**yaml.load(doc))
          if description:
            property.description = _format_docstring(description)
        model.add_property(property)
      models[model.name] = model.to_dict()
    return models




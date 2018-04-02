import yaml
import inspect
import datetime
import collections

from enum import Enum
from functools import wraps

from swagger.base import Model, Property, Path, Operation, Parameter, Response
from swagger.base import Content, RequestBody
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
  try:
    if isinstance(data_type, str):
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
  return ' '.join([l.strip() for l in docstring.strip().split('\n')])


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
    self.__operations = {}
    self.__handlers = []
    self.__models = []
    self.__enums = []
    self.__properties = collections.defaultdict(list)
    self.__specs = None

  def register_operations(self, app):
    for r in app.wildcard_router.rules:
      for name, member in inspect.getmembers(r.target):
        if hasattr(member, 'func_args'):
          self.__handlers.append(r.target)
          path = r.matcher._path%tuple('{%s}'%a for a in member.func_args)
          ops = self.__operations.setdefault(r.target, dict(path=path, methods=[]))
          ops['methods'].append(member)

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
    spec['paths'] = self._parse_paths()
    return spec

  def _parse_paths(self):
    paths = []
    for hdlr in self.__handlers:
      if hdlr not in self.__operations:
        self.logger.debug('Handler %s has no operations registered'%hdlr.__name__)
        continue
      in_path_params = []
      if hdlr.__doc__:
        _, params = hdlr.__doc__.split('---')
        for p in yaml.load(params):
          in_path_params.append(Parameter(**p, show_in='path'))
      tag = hdlr.__module__.split('.')[0]
      tag = tag[0].upper() + tag[1:]
      path, methods = self.__operations[hdlr]['path'], self.__operations[hdlr]['methods']
      path = Path(path)
      for m in methods:
        op = Operation(tag, m.__name__)
        if m.__doc__:
          summary, method_specs = m.__doc__.split('---')
          op.summary, method_specs = _format_docstring(summary), yaml.load(method_specs)
          request_body = method_specs.get('request_body', None)
          if request_body:
            op.request_body = RequestBody(Content(request_body['content']))
          for p in in_path_params:
            op.add_parameter(p)
          for name, p in method_specs.get('parameters', {}).items():
            op.add_parameter(Parameter(name=name, **p))
          for code, r in method_specs.get('responses', {}).items():
            op.add_response(Response(code=code, description=r.get('description', ''),
                                     content=Content(r['content'])))
        path.add_operation(op)
      paths.append(path)
    return {p.path: p.to_dict() for p in paths}

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
    # add message and error models
    for m in (self._mesasge_model(), self._error_model()):
      models[m.name] = m.to_dict()
    return models

  def _mesasge_model(self):
    model = Model('Message', description='PIVOT normal message')
    p = Property('message')
    p.update(type=str, read_only=True, description='Message body')
    model.add_property(p)
    return model

  def _error_model(self):
    model = Model('Error', description='PIVOT error message')
    p = Property('error')
    p.update(type=str, read_only=True, description='Error message body')
    model.add_property(p)
    return model




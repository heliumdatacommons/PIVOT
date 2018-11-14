import yaml
import inspect
import datetime
import collections

from functools import wraps

from commons import Loggable, Singleton


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
  set: dict(type='array'),
  object: dict(type='object')
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
          request_body = method_specs.get('request_body')
          if request_body:
            op.request_body = RequestBody(Content(request_body['content']))
          for p in in_path_params:
            op.add_parameter(p)
          for p in method_specs.get('parameters', []):
            op.add_parameter(Parameter(**p, show_in=p.pop('in', None)))
          for code, r in method_specs.get('responses', {}).items():
            resp = Response(code=code, description=r.get('description', ''))
            content = r.get('content')
            if content:
              resp.content = Content(content)
            op.add_response(resp)
        path.add_operation(op)
      paths.append(path)
    return {p.path: p.to_dict() for p in paths}

  def _parse_enums(self):
    from enum import Enum
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



class Path:

  def __init__(self, path, ops=[]):
    self.__path = path
    self.__ops = list(ops)

  @property
  def path(self):
    return self.__path

  def add_operation(self, op):
    self.__ops.append(op)

  def to_dict(self):
    return {op.method: op.to_dict() for op in self.__ops}


class Operation:

  def __init__(self, tag, method, summary='', request_body=None, params=[], responses=[]):
    self.__tag = tag
    self.__method = method
    self.__summary = summary
    self.__request_body = request_body
    self.__params = list(params)
    self.__responses = list(responses)

  @property
  def method(self):
    return self.__method

  @property
  def summary(self):
    return self.__summary

  @property
  def request_body(self):
    return self.__request_body

  @summary.setter
  def summary(self, summary):
    self.__summary = summary

  @request_body.setter
  def request_body(self, request_body):
    self.__request_body = request_body

  def add_parameter(self, p):
    self.__params.append(p)

  def add_response(self, r):
    self.__responses.append(r)

  def to_dict(self):
    op = dict(tags=[self.__tag],
              parameters=[p.to_dict() for p in self.__params],
              responses={r.code: r.to_dict() for r in self.__responses})
    if self.__summary:
      op.update(summary=self.__summary)
    if self.__request_body:
      op.update(requestBody=self.__request_body.to_dict())
    return op


class RequestBody:

  def __init__(self, content):
    self.__content = content

  def to_dict(self):
    return dict(content=self.__content.to_dict())


class Parameter:

  def __init__(self, name, type, show_in, description='', items=None, required=False,
               *args, **kwargs):
    self.__name = name
    self.__type = type
    self.__in = show_in
    self.__description = description
    self.__items = items
    self.__required = required

  @property
  def name(self):
    return self.__name

  @property
  def show_in(self):
    return self.__in

  @show_in.setter
  def show_in(self, show_in):
    self.__in = show_in

  def to_dict(self):
    res = {'name': self.__name, 'in': self.__in,
           'schema': _convert_data_type(self.__type)}
    if self.__required:
      res.update(required=self.__required)
    if self.__description:
      res.update(description=self.__description)
    if self.__items:
      res.update(items=_convert_data_type(self.__items))
    return res


class Response:

  def __init__(self, code, content=None, description=''):
    self.__code = code
    self.__content = content
    self.__description = description

  @property
  def code(self):
    return self.__code

  @property
  def content(self):
    return self.__content

  @content.setter
  def content(self, content):
    self.__content = content


  def to_dict(self):
    resp = {}
    if self.__content:
      resp.update(content=self.__content.to_dict())
    if self.__description:
      resp.update(description=self.__description)
    return resp


class Content:

  def __init__(self, schemas):
    self.__schemas = dict(schemas)

  def _parse_schema(self, schema):
    if isinstance(schema, dict):
      scm = {}
      type = schema.pop('type', None)
      if type:
        scm.update(**_convert_data_type(type))
      items = schema.pop('items', None)
      if items:
        scm.update(items=_convert_data_type(items))
      return scm
    return _convert_data_type(schema)

  def to_dict(self):
    return {fmt: dict(schema=self._parse_schema(scm['schema']))
            for fmt, scm in self.__schemas.items()}


# class Enum:
#
#   def __init__(self, name, type, values=[]):
#     self.__name = name
#     self.__type = type
#     self.__values = list(values)
#
#   def to_dict(self):
#     return {self.__name: dict(type=self.__type, enum=list(self.__values))}


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
    self.__type = type and _convert_data_type(type, self.__additional_properties)

  @description.setter
  def description(self, description):
    self.__description = description

  def update(self, type=None, description=None, items=None, required=False,
             nullable=False, additional_properties=None, default=None,
             read_only=False, write_only=False, example=None,
             maximum=None, minimum=None, **kwargs):
    assert not (read_only and write_only)
    self.__type = type and _convert_data_type(type, additional_properties)
    self.__description = description
    self.__items = items and _convert_data_type(items, additional_properties)
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



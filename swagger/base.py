import swagger
import datetime


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

  @summary.setter
  def summary(self, summary):
    self.__summary = summary

  @property
  def request_body(self):
    return self.__request_body

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
    if self.__request_body:
      op.update(requestBody=self.__request_body.to_dict())
    return op


class RequestBody:

  def __init__(self, content):
    self.__content = content

  def to_dict(self):
    return dict(content=self.__content.to_dict())


class Parameter:

  def __init__(self, name, type, show_in, description='', required=False):
    self.__name = name
    self.__type = type
    self.__in = show_in
    self.__description = description
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
           'schema': swagger._convert_data_type(self.__type)}
    if self.__required:
      res.update(required=self.__required)
    if self.__description:
      res.update(description=self.__description)
    return res


class Response:

  def __init__(self, code, content, description=''):
    self.__code = code
    self.__content = content
    self.__description = description

  @property
  def code(self):
    return self.__code

  def to_dict(self):
    resp = dict(content=self.__content.to_dict())
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
        scm.update(**swagger._convert_data_type(type))
      items = schema.pop('items', None)
      if items:
        scm.update(items=swagger._convert_data_type(items))
      return scm
    return swagger._convert_data_type(schema)

  def to_dict(self):
    return {fmt: dict(schema=self._parse_schema(scm['schema']))
            for fmt, scm in self.__schemas.items()}


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

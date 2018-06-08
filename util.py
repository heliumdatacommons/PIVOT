import os
import dateutil.parser


def dirname(f):
  return os.path.dirname(os.path.abspath(f))


def message(msg):
  return dict(message=msg)


def error(msg):
  return dict(error=msg)


def parse_datetime(d):
  if isinstance(d, str):
    return dateutil.parser.parse(d)
  return d


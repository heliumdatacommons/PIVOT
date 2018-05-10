import os
import re
import dateutil.parser


def dirname(f):
  return os.path.dirname(os.path.abspath(f))


def message(msg):
  return dict(message=msg)


def error(msg):
  return dict(error=msg)


def parse_container_short_id(p, appliance):
  return re.sub(r'(.*)\@([a-z0-9\.-]+)(.*)',
                r'\1\2-%s.marathon.containerip.dcos.thisdcos.directory\3'%appliance,
                str(p))


def parse_datetime(d):
  if isinstance(d, str):
    return dateutil.parser.parse(d)
  return d



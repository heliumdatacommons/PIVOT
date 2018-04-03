#! /usr/bin/env python3

import os
import sys
import time
import json
import toml
import stat
import socket
import subprocess
import multiprocessing

from argparse import ArgumentParser


def parse_args():
  parser = ArgumentParser(description="Launch PIVOT")
  parser.add_argument('-m', '--master', dest='dcos_master', type=str, required=True,
                      help='DC/OS master')
  parser.add_argument('-t', '--token', dest='dcos_token', type=str, required=True,
                      help='DC/OS authentication token')
  parser.add_argument('-p', '--port', dest='pivot_port', type=int, default=9090,
                      help='PIVOT listen port')
  parser.add_argument('-n', '--n_parallel', dest='pivot_n_parallel', type=int,
                      default=multiprocessing.cpu_count(),
                      help='PIVOT parallelism level')
  return parser.parse_args()


def create_dcos_config(dcos_master, dcos_token):
  dcos_cfg = dict(
    core=dict(
      dcos_url=dcos_master if dcos_master.startswith('http') else 'http://%s'%dcos_master,
      dcos_acs_token=dcos_token,
      dcos_auto_login=str(True),
      ssl_verify=str(True))
  )
  dcos_cfg_dir = os.path.expanduser('~/.dcos')
  dcos_cfg_f = '%s/dcos.toml'%dcos_cfg_dir
  os.makedirs(dcos_cfg_dir, exist_ok=True)
  toml.dump(dcos_cfg, open(dcos_cfg_f, 'w'))
  os.chmod(dcos_cfg_f, stat.S_IREAD | stat.S_IWRITE)


def create_pivot_config(dcos_master, pivot_port, pivot_n_parallel):
  pivot_cfg_f = '/opt/pivot/config.json'
  pivot_cfg = json.load(open(pivot_cfg_f))
  pivot_cfg['dcos']['master_url'] = dcos_master if dcos_master.startswith('http') else 'http://%s'%dcos_master
  pivot_cfg.update(dict(port=pivot_port, n_parallel=pivot_n_parallel))
  json.dump(pivot_cfg, open(pivot_cfg_f, 'w'))


def check_mongodb_port():
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  return sock.connect_ex(('127.0.0.1', 27017)) == 0


def run_pivot():
  try:
    subprocess.run('/etc/init.d/mongodb start', shell=True, check=True,
                   stdout=sys.stdout, stderr=sys.stderr)
    while not check_mongodb_port():
      sys.stdout.write('Wait for MongoDB\n')
      sys.stdout.flush()
      time.sleep(3)
    sys.stdout.write('MongoDB is ready\n')
    sys.stdout.flush()
    subprocess.run('python3 /opt/pivot/server.py', shell=True, check=True,
                   stdout=sys.stdout, stderr=sys.stderr)
  except subprocess.CalledProcessError as e:
    sys.exit(e.returncode)


if __name__ == '__main__':
  args = parse_args()
  create_dcos_config(args.dcos_master, args.dcos_token)
  create_pivot_config(args.dcos_master, args.pivot_port, args.pivot_n_parallel)
  run_pivot()



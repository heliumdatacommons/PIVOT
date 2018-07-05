#! /usr/bin/env python3

import sys
import time
import yaml
import socket
import subprocess
import multiprocessing

from argparse import ArgumentParser


def parse_args():
  parser = ArgumentParser(description="Launch PIVOT")
  parser.add_argument('--master', dest='master', type=str, required=True,
                      help='Mesos/Marathon master host')
  parser.add_argument('--port', dest='port', type=int, default=9090,
                      help='PIVOT listen port')
  parser.add_argument('--n_parallel', dest='n_parallel', type=int,
                      default=multiprocessing.cpu_count(),
                      help='PIVOT parallelism level')
  parser.add_argument('--db_host', dest='db_host', type=str,
                      default='pivot-db-sys.marathon.containerip.dcos.thisdcos.directory',
                      help='MongoDB host')
  parser.add_argument('--db_port', dest='db_port', type=int, default=27017,
                      help='MongoDB listen port')
  parser.add_argument('--db_name', dest='db_name', type=str, default='pivot',
                      help='Database name')

  return parser.parse_args()


def create_pivot_config(args):
  pivot_cfg_f = '/opt/pivot/config.yml'
  pivot_cfg = yaml.load(open(pivot_cfg_f))
  pivot_cfg['pivot'].update(master=args.master,
                            port=args.port,
                            n_parallel=args.n_parallel)
  pivot_cfg['db'] = dict(host=args.db_host, port=args.db_port, name=args.db_name)
  yaml.dump(pivot_cfg, open(pivot_cfg_f, 'w'), default_flow_style=False)


def check_mongodb_port(args):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  return sock.connect_ex((args.db_host, args.db_port)) == 0


def run_pivot(args):
  try:
    while not check_mongodb_port(args):
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
  create_pivot_config(args)
  run_pivot(args)



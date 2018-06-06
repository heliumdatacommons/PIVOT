#! /usr/bin/env python3

import sys
import subprocess

from argparse import ArgumentParser


def parse_args():
  parser = ArgumentParser(description="Launch MongoDB")
  parser.add_argument('--port', dest='port', type=int, default=27017,
                      help='MongoDB listen port')
  return parser.parse_args()


def run_mongodb(args):
  try:
    subprocess.run('/usr/bin/mongod --bind_ip=0.0.0.0 --port=%d'%args.port,
                   shell=True, check=True,
                   stdout=sys.stdout, stderr=sys.stderr)
  except subprocess.CalledProcessError as e:
    sys.exit(e.returncode)


if __name__ == '__main__':
  args = parse_args()
  run_mongodb(args)



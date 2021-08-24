#!/usr/bin/env python3
import argparse
import json
import os
import shlex
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(__file__)
WSKISH = os.path.join(SCRIPT_DIR, "../../../wskish/wskish")
WSKGEN = os.path.join(SCRIPT_DIR, "../../../runtime/scripts/wskgen")
FAAS = os.path.join(SCRIPT_DIR, "../../faas-profiler.sh")


ACTION_FMT = '''
import time

def main(params):
    time.sleep({})
    return {{"index": "1"}}
'''
ACTION = '/tmp/action.py'



def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("num", help="directory of object files to run", type=int)
  parser.add_argument("sleep", help="how long each action should sleep for in ms", type=int)
  parser.add_argument("host", help="hostname:port combo")
  args = parser.parse_args()

  with open(ACTION, 'w') as f:
    f.write(ACTION_FMT.format(args.sleep / 1000))

  acts = [ '/whisk.system/action{}'.format(i) for i in range(args.num) ]
  seq = ','.join(acts)
  for act in acts:
    run_cmd('wsk -i action delete {}'.format(act), fail_ok=True)
    run_cmd('wsk -i action create {} {}'.format(act, ACTION))

  run_cmd('wsk -i action delete chain', fail_ok=True)
  run_cmd('wsk -i action create chain --sequence {}'.format(seq))


def run_cmd(c, fail_ok=False):
  print("running {}".format(c))
  try:
    subprocess.check_call(shlex.split(c))
  except Exception as e:
    if not fail_ok:
      raise e

if __name__ == "__main__":
  main()

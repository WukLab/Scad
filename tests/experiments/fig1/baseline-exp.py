#!/usr/bin/env python3
import argparse
import json
import subprocess
import shlex

BASELINE_FILE = '/tmp/action.py'
BASELINE_PY = '''
import time

def main(action):
    time.sleep({sleep_time})
    return {{"index": "1"}}
'''

WORKLOAD_FILE = '/tmp/workload.json'
WORKLOAD = {
    "test_name": None,
    "test_duration_in_seconds": 60,
    "random_seed": 100,
    "blocking_cli": False,
    "instances":{
        "instance1":{
            "application": "test-action",
            "distribution": "Uniform",
            "rate": 1,
            "activity_window": [5, 55]
        }
    },
    "perf_monitoring":{
        "runtime_script": "monitoring/RuntimeMonitoring.sh",
        "post_script": None
    }
}

def create_chain(chain_length, sleep_time, host):

  for i in range(chain_length):
    action_str = BASELINE_PY.format(sleep_time=sleep_time)
    with open(BASELINE_FILE, 'w') as f:
      f.truncate()
      f.write(action_str)
    # first, delete all potential actions
    try:
      subprocess.check_call(shlex.split("wsk -i --apihost {} action delete obj{}".format(host, i)))
    except:
      # ok to fail
      pass
    # next create new action
    subprocess.check_call(shlex.split("wsk -i --apihost {} action create obj{} {}".format(host, i, BASELINE_FILE)))

  obj_names = ','.join(list(map(lambda x: '/whisk.system/obj{}'.format(x), range(chain_length))))
  print(obj_names)
  # finally, create the sequence
  try:
      subprocess.check_call(shlex.split("wsk -i --apihost {} action delete test-action".format(host, i)))
  except:
    # ok to fail
    pass
  subprocess.check_call(shlex.split("wsk -i --apihost {} action create test-action --sequence {}".format(host, obj_names)))




def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("chain_length", type=int, help="sequence function chain length")
  parser.add_argument("sleep_time", type=float, help="simulated function runtime (in seconds)")
  parser.add_argument("--host", type=str, help="controller host", default="wuklab-06.ucsd.edu")
  args = parser.parse_args()
  create_chain(args.chain_length, args.sleep_time, args.host)
  subprocess.check_call(shlex.split("wsk -i --apihost {} action list".format(args.host)))
  subprocess.check_call(shlex.split("wsk -i --apihost {} action invoke test-action -b".format(args.host)))
  WORKLOAD['test_name'] = "baseline-ow-{}-{}".format(args.chain_length, int(1000*args.sleep_time))
  with open(WORKLOAD_FILE, 'w') as f:
    f.truncate()
    f.write(json.dumps(WORKLOAD, indent=2))
  print("wrote workload {} to {}".format(WORKLOAD['test_name'], WORKLOAD_FILE))

if __name__ == "__main__":
  main()
#!/usr/bin/env python3
import argparse
import json
import os
import shlex
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(__file__)
WSKISH = os.path.join(SCRIPT_DIR, "../../wskish/wskish")
WSKGEN = os.path.join(SCRIPT_DIR, "../../runtime/scripts/wskgen")
FAAS = os.path.join(SCRIPT_DIR, "../faas-profiler.sh")

ACTION = '/tmp/action.json'

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

def write_workload(name):
  os.remove(WORKLOAD_FILE)
  with open(WORKLOAD_FILE, 'x') as f:
    WORKLOAD["test_name"] = name
    f.write(json.dumps(WORKLOAD, indent=2))

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("experiment_dir", help="directory of object files to run")
  parser.add_argument("host", help="hostname:port combo ")
  parser.add_argument("workload", help="faas profiler workload.json ")

  args = parser.parse_args()
  if not os.path.exists(args.experiment_dir) and not os.path.isdir(args.experiment_dir):
    print("experiment dir must exist")
  if not os.path.exists(args.workload) and not os.path.isfile(args.workload):
    print("workload file must exist")

  print("wskgen")
  subprocess.check_call(shlex.split("{} -o {} {}".format(WSKGEN, ACTION, args.experiment_dir)))
  print("delete prev action")
  subprocess.check_call(shlex.split("{} --host {} delete".format(WSKISH, args.host)))
  print("put new action")
  subprocess.check_call(shlex.split("{} --host {} --file {} put".format(WSKISH, args.host, ACTION)))
  print("running faas")
  write_workload(args.workload)
  subprocess.check_call(shlex.split("{} WorkloadInvoker -c {}".format(FAAS, WORKLOAD_FILE)))

if __name__ == "__main__":
  main()
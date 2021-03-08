#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import print_function
from sys import stdin
from sys import stdout
from sys import stderr
from os import fdopen
import sys, os, json, traceback, warnings

####################
# BEGIN libd runtime
####################

from disagg import LibdAction
import threading
import struct
import os

class LibdRuntime:
    def init(self):
        self.actions = {}

    def add_action(self, name, action):
        # TODO: inject APIs into this object
        action.request = None
        self.actions[name] = action

    # This may throw exception
    def get_action(self, name):
        return self.actions[name]

    def terminate_action(self, name):
        # TODO: call of dealloc is not garenteed, use ternimate?
        del self.actions[name]


# Params: a list of strings. Body: the body of http request
def _act_add        (runtime, params, body):
    runtime.add_action(LibdAction(*params))
def _trans_add      (runtime, params, body):
    runtime.get_action(name).add_transport(*params)
def _trans_config   (runtime, params, body):
    runtime.get_action(name).config_transport(*params)

cmd_funcs = {
    # create action
    'ACTADD'    : _act_add,
    'TRANSADD'  : _trans_add,
    'TRANSCONF' : _trans_config
}

def handle_message(fifoName, runtime):
    try:
        fifo = os.open(fifoName, os.O_RDONLY)
        while True:
            # parse message from FIFO
            size = struct.unpack("<I", os.read(fifo, 4))[0]
            content = os.read(fifo, size).decode('ascii')
            msg = json.loads(content)
            print(msg)

            # forward message to json
            body = json.dumps(msg.body)
            cmd_funcs[msg.cmd](runtime, msg.params, body)
    finally:
        print('cannot open file fifo', file=stderr)

# start libd monitor thread, this will keep up for one initd function
FIFO_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "../fifo")
_runtime = LibdRuntime()
threading.Thread(target=handle_message, args=(FIFO_FILE, _runtime)).start()

####################
# END   libd runtime
####################

try:
  # if the directory 'virtualenv' is extracted out of a zip file
  path_to_virtualenv = os.path.abspath('./virtualenv')
  if os.path.isdir(path_to_virtualenv):
    # activate the virtualenv using activate_this.py contained in the virtualenv
    activate_this_file = path_to_virtualenv + '/bin/activate_this.py'
    if os.path.exists(activate_this_file):
      with open(activate_this_file) as f:
        code = compile(f.read(), activate_this_file, 'exec')
        exec(code, dict(__file__=activate_this_file))
    else:
      sys.stderr.write('Invalid virtualenv. Zip file does not include /virtualenv/bin/' + os.path.basename(activate_this_file) + '\n')
      sys.exit(1)
except Exception:
  traceback.print_exc(file=sys.stderr, limit=0)
  sys.exit(1)

# now import the action as process input/output
from main__ import main as main

out = fdopen(3, "wb")
if os.getenv("__OW_WAIT_FOR_ACK", "") != "":
    out.write(json.dumps({"ok": True}, ensure_ascii=False).encode('utf-8'))
    out.write(b'\n')
    out.flush()

env = os.environ
while True:
  line = stdin.readline()
  if not line: break
  args = json.loads(line)
  payload = {}
  for key in args:
    if key == "value":
      payload = args["value"]
    elif key == 'cmds':
      for cmd in args['cmds']:
        cmd_funcs[cmd.cmd](_runtime, cmd.params, cmd.body)
    else:
      env["__OW_%s" % key.upper()]= args[key]
  res = {}
  # Here the funciton is in the same thread
  try:
    res = main(payload)
  except Exception as ex:
    print(traceback.format_exc(), file=stderr)
    res = {"error": str(ex)}
  out.write(json.dumps(res, ensure_ascii=False).encode('utf-8'))
  out.write(b'\n')
  stdout.flush()
  stderr.flush()
  out.flush()

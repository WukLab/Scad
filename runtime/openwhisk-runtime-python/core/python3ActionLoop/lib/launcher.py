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
import threading, struct, os

def debug(*args):
    print(*args, file=stderr)
    stderr.flush()
# class LibdRequest:
#     def __init__(self, aid, serverUrl):
#         self.aid = aid
#         self.surl = serverUrl
#     def __ep(self, api, data):
#         url = "{}/activation/{}/{}".format(self.surl, self.aid, api)
#         return requests.post(api, json.dumps(data))
#     def dependency(target = null, value = null, parallelism = null, dependency = null, functionActivationId = null, appActivationId = null):
#         data = {}

class LibdRuntime:
    def __init__(self):
        self.actions = {}
        self.stash_msgs = {}
        self.server_url = os.getenv('__OW_INVOKER_API_URL', 'localhost')
        self.cv = threading.Condition()
        self.fifo = fifoOut
    def stash(self,aid,func,*args):
        print("stash request", aid, func, args, file=stderr)
        stderr.flush()
        self.stash_msgs.setdefault(aid, []).append((func, args))
    def unstash(self, aid):
        print("unstash request", aid, file=stderr)
        stderr.flush()
        if aid in self.stash_msgs:
            for func,args in self.stash_msgs[aid]:
                func(self, *args)
            del self.stash_msgs[aid]

    # all those functions will write a message
    def create_action(self, aid):
        args = {"post_url":'http://172.17.0.1:2400'}
        action = LibdAction(self.cv, aid, **args)
        self.actions[aid] = action

        return action
    def get_action(self, aid):
        return self.actions[aid]
    def terminate_action(self, name):
        # TODO: call of dealloc is not garenteed, use ternimate?
        action.terminate()
        del self.actions[name]


# start libd monitor thread, this will keep up for one initd function
FIFO_IN_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "../fifoIn")
FIFO_OUT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "../fifoIn")

fifoIn  = os.open(inFifo, os.O_RDONLY)
fifoOut = os.open(outFifo, os.O_WRONLY)

_runtime = LibdRuntime()
threading.Thread(target=handle_message, args=(fifoIn, _runtime)).start()

# Params: a list of strings. Body: the body of http request
# TODO: consider adding lock here
def _act_msgs       (runtime, params, body = None):
    msgs = {}
    action = runtime.get_action (params[0])
    for t in action.raw_transports:
        size, msg = t.get_msg()
        if size > 0:
            msgs[t.name] = msg
    rep = json.dumps({'ok': True, 'messages': msgs})
    debug('collect messages', rep)
    os.write(fifoOut, struct.pack("<I", len(rep)))
    os.write(fifoOut, msg_json.encode('ascii'))
    debug('send message', message)
def _act_add        (runtime, params, body):
    runtime.create_action(params['activation_id'])
def _trans_add      (runtime, params, body):
    runtime.get_action(params[0]).add_transport(**body)
    _act_msgs(runtime, params)
def _trans_config   (runtime, params, body):
    debug('try to config', params, body)
    if params[0] in runtime.actions:
        action = runtime.get_action(params[0])
        action.config_transport(params[1], body['durl'])
        debug('finish config', params, body)
        _act_msgs(runtime, params)
    else:
        runtime.stash(params[0], _trans_config, params, body)

cmd_funcs = {
    # create action
    'ACTADD'    : _act_add,
    'TRANSADD'  : _trans_add,
    'TRANSCONF' : _trans_config,
    'ACTMSGS' : _act_msgs
}

# Open FIFOs

def handle_message(fifoIn, runtime):
    try:
        while True:
            stderr.write('Listen on fifo file ' + fifoName + '\n')
            stderr.flush()
            # parse message from FIFO
            size = struct.unpack("<I", os.read(fifoIn, 4))[0]
            content = os.read(fifoIn, size).decode('ascii')
            msg = json.loads(content)
            body = json.loads(msg.get('body', "{}"))
            params = msg.get('params', [])
            debug('get cmd', params, body)
            cmd_funcs[msg['cmd']](runtime, params, body)
    finally:
        print('Error happens in FIFO thread', file=stderr)
        stderr.flush()
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
  # TODO: log error at this phase
  payload = {}
  action = None
  aid = None
  transports = []
  for key in args:
    stderr.flush()
    if key == "value":
      payload = args["value"]
    elif key == 'activation_id':
      aid = args['activation_id']
      action = _runtime.create_action(args['activation_id'])
    elif key == 'transports':
      transports = args['transports']
    else:
      env["__OW_%s" % key.upper()]= args[key]
  if action != None and transports != None:
    for trans in transports:
      action.add_transport(trans)
      # XXX: we do not message here
      # we do not expect to get parameters here
      # message(action, trans, ???)
    _runtime.unstash(aid)
    # TODO: stasjed messages? we'd expect no stashed messages

  res = {}
  # Here the funciton is in the same thread
  try:
    res = main(payload, action)
  except Exception as ex:
    print(traceback.format_exc(), file=stderr)
    res = {"error": str(ex)}
  resjson = json.dumps(res, ensure_ascii=False).encode('utf-8')
  out.write(resjson)
  out.write(b'\n')
  stdout.flush()
  stderr.flush()
  out.flush()
  # terminate actions after finish?
  if action != None:
    _runtime.terminate_action(aid)


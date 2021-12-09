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
from multiprocessing import Pipe
# for http requests

def debug(*args):
    print(*args, file=stderr)
    stderr.flush()

# Params: a list of strings. Body: the body of http request
# TODO: consider adding lock here

# start libd monitor thread, this will keep up for one initd function
FIFO_IN_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "../fifoIn")
FIFO_OUT_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "../fifoOut")

# TODO: why use a FD 3 here?
out = fdopen(3, "wb")

fifoIn  = os.open(FIFO_IN_FILE, os.O_RDONLY)
fifoOut = os.open(FIFO_OUT_FILE, os.O_WRONLY)

def _act_msgs       (action, params, body = None):
    msgs = {}
    for name, t in action.raw_transports.items():
        size, msg = t.get_msg()
        if size > 0:
            msgs[name] = msg
    rep = json.dumps({'ok': True, 'messages': msgs})
    os.write(fifoOut, struct.pack("<I", len(rep)))
    os.write(fifoOut, rep.encode('ascii'))
    debug('send message', rep)
def _act_add        (action, params, body):
    runtime.create_action(params[0])
def _trans_add      (action, params, body):
    action.add_transport(**body)
    _act_msgs(runtime, params)
def _trans_config   (action, params, body):
    action.config_transport(params[1], body['durl'])
    debug('finish config', params, body)

cmd_funcs = {
    # create action
    'ACTADD'    : _act_add,
    'TRANSADD'  : _trans_add,
    'TRANSCONF' : _trans_config,
    'ACTMSGS' : _act_msgs
}

def _proxy_send(cmd):
    def send(pconn, params, body = None):
        pconn.send([cmd, params, body])
    return send
def _proxy_act_msgs(pconn, params, body = None):
    pconn.send(['ACTMSGS', params, body])
    msgs = pconn.recv()
    rep = json.dumps({'ok': True, 'messages': msgs})
    os.write(fifoOut, struct.pack("<I", len(rep)))
    os.write(fifoOut, rep.encode('ascii'))
    debug('send message', rep)

proxy_funcs = {
    # create action
    'ACTADD'    : _proxy_send('ACTADD'),
    'TRANSADD'  : _proxy_send('TRANSADD'),
    'TRANSCONF' : _proxy_send('TRANSCONF'),
    'ACTMSGS' : _proxy_act_msgs
}

class LibdRuntime:
    def __init__(self):
        self.actions = {}
        self.action_funcs = {}
        self.stash_msgs = {}
        self.server_url = os.getenv('__OW_INVOKER_API_URL', 'localhost')
        self.cv = threading.Condition()
        self.fifo = fifoOut
        # self.action_args = {"post_url":'http://172.17.0.1:2400'}

    # runtime services
    def stash(self,aid,cmd,args):
        debug("stash request", aid, cmd, args)
        self.stash_msgs.setdefault(aid, []).append((cmd, args))
    def unstash(self, aid):
        debug("unstash request", aid)
        action = self.actions[aid]
        if aid in self.stash_msgs:
            for cmd,args in self.stash_msgs.get(aid, []):
                debug("unstash command", cmd)
                self.action_funcs[aid][cmd](action, *args)
            del self.stash_msgs[aid]
    def execute(self,aid,cmd,*args):
        debug("execute requests", aid,cmd,args)
        if aid in self.actions:
            self.action_funcs[aid][cmd](self.actions[aid], *args)
        else:
            self.stash(aid,cmd,args)

    # all those functions will write a message
    def _create_action(self, aid, transports, config):
        params = {}
        if 'action_name' in config:
            params['name'] = '/'.join(config['action_name'].split('/')[-2:])
        if 'profile' in config:
            params['post_url'] = config['profile']
            params['plugins'] = 'monitor'
        # prepare action params
        debug("init action with config list" + str(params))
        action = LibdAction(self.cv, aid, **params)
        action.runtime = self
        self.actions[aid] = action
        # add transports
        for t in transports:
            action.add_transport(t)
        # unstash messages
        self.action_funcs[aid] = cmd_funcs
        self.unstash(aid)
        return action
    def _create_proxy(self, aid, transports, config):
        pconn, cconn = Pipe()
        pconn.send(['ACTADD', [aid, transports], None])
        self.actions[aid] = pconn

        self.action_funcs[aid] = proxy_funcs
        self.unstash(aid)
        return pconn
    def create_action(self, aid, transports, config):
        if not config['merged']:
            return self._create_action(aid, transports, config)
        else:
            return self._create_proxy(aid, transports, config)

    def get_action(self, aid):
        return self.actions[aid]
    def terminate_action(self, name):
        # TODO: call of dealloc is not garenteed, use ternimate?
        action.terminate()
        del self.actions[name]

def handle_message(fifoIn, runtime):
    try:
        while True:
            debug('Listen on input FIFO')
            # parse message from FIFO
            size = struct.unpack("<I", os.read(fifoIn, 4))[0]
            content = os.read(fifoIn, size).decode('ascii')
            msg = json.loads(content)
            body = json.loads(msg.get('body', "{}"))
            params = msg.get('params', [])
            debug('get cmd', msg['cmd'], params, body)

            runtime.execute(params[0], msg['cmd'], params, body)
    finally:
        debug('Error happens in FIFO thread')

_runtime = LibdRuntime()
threading.Thread(target=handle_message, args=(fifoIn, _runtime)).start()

# Open FIFOs

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
  config_keys = ['profile', 'merged', 'action_name']
  config = {'merged': False}
  for key in args:
    stderr.flush()
    if key == "value":
      payload = args["value"]
    elif key == 'activation_id':
      aid = args['activation_id']
    elif key == 'transports':
      transports = args['transports']
    elif key in config_keys:
      config[key] = args[key]
    else:
      env["__OW_%s" % key.upper()]= args[key]
  action = _runtime.create_action(aid, transports, config)

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


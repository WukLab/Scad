import os
import struct
import json
from disagg import *

def encode_msg_size(size):
    return struct.pack("<I", size)

def decode_msg_size(size_bytes):
    return struct.unpack("<I", size_bytes)[0]

# parames should be list of string
def create_msg(cmd, params):
    content = json.dumps({ 'cmd': cmd, 'params': params }).encode('ascii')
    size = struct.pack("<I", len(content))
    return size + content

def fetch_msg(fifo):
    size = struct.unpack("<I", os.read(fifo, 4))[0]
    content = os.read(fifo, size).decode('ascii')
    return json.loads(content)

def _act_add(runtime, *params):
    runtime.add_action(LibdAction(*params))
def _trans_add(runtime, name, *params):
    runtime.get_action(name).add_transport(*params)
def _trans_config(runtime, name, *params):
    runtime.get_action(name).config_transport(*params)

cmd_funcs = {
    # create action
    'ACTADD'    : _act_add,
    'TRANSADD'  : _trans_add,
    'TRANSCONF' : _trans_config
}

def handle_message(fifoName, runtime):
    with os.open(fifoName, os.O_RDONLY) as fifo:
        # TODO: ternimate?
        while True:
            msg = fetch_msg(fifo)
            print(msg)
            # simple IPC
            cmd_funcs[msg.cmd](runtime, *msg.params)

# Libd object infomation
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


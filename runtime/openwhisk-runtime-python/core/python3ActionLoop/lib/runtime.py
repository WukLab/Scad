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

ACTADD PARAMS
TRANSADD ACTION,PARAMS
TRANSCONF ACTION,PARAMS

def handle_message(fifoName, runtime):
    action functions
    def _act_add(*params):
        runtime.add_action(LibdAction(*params))
    def _trans_add(name, *params):
        runtime.get_action(name).add_transport(*params)
    def _trans_config(name, *params):
        runtime.get_action(name).config_transport(*params)

    funcs = {
        # create action
        'ACTADD'    : _act_add,
        'TRANSADD'  : _trans_add,
        'TRANSCONF' : _trans_config
    }

    with os.open(fifoName, os.O_RDONLY) as fifo:
        # TODO: ternimate?
        while True:
            msg = fetch_msg(fifo)
            print(msg)
            # simple IPC
            funcs[msg.cmd](*msg.params)


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


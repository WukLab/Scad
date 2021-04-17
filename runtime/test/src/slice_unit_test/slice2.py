import struct
import threading
import time
import pickle
import random
import sys
import copy
import codecs
import copyreg
import collections
import numpy as np
import json
import jsonpickle
import base64
from random import randrange
from types import SimpleNamespace
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

def read_params():
    with open('context_slice.json', 'r') as outfile:
        return json.load(outfile)
    return {}

def write_params(params):
    with open('context_slice.json', 'w') as outfile:
        json.dump(params, outfile)

def main(params, action):
    transport_name = 'client1'
    trans = action.get_transport(transport_name, 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)
    context_dict_in_b64 = params["func1"]
    context_dict = pickle.loads(base64.b64decode(context_dict_in_b64))
    buffer_pool = buffer_pool_lib.buffer_pool(trans, context_dict["buffer_pool_metadata"])
    remote_array_metadata = context_dict["remote_array"]
    print("original metadata")
    print("page_id_list: {0}".format(remote_array_metadata.page_id_list))
    print("begin_offset: {0}, end_offset {1}".format(remote_array_metadata.begin_offset, remote_array_metadata.end_offset))
    remote_array_instance = remote_array(buffer_pool, metadata=remote_array_metadata)
    remote_slice_instance = remote_array_instance.get_slice(40, 180)
    print("slice metadata")
    print("page_id_list: {0}".format(remote_slice_instance.metadata.page_id_list))
    print("begin_offset: {0}, end_offset {1}".format(remote_slice_instance.metadata.begin_offset, remote_slice_instance.metadata.end_offset))
    remote_slice_array = remote_slice_instance.materialize()
    print(remote_slice_array)

action = buffer_pool_lib.action_setup()
params = read_params()
main(params, action)


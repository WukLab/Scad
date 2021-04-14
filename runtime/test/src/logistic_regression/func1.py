import struct
import threading
import time
import pickle
import sys
import copy
import codecs
import copyreg
import collections
import numpy as np
import json
import jsonpickle
import base64
from types import SimpleNamespace
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

def write_params(params):
    with open('context.json', 'w') as outfile:
        json.dump(params, outfile)

def main(params, action):
    # setup
    action = buffer_pool_lib.action_setup()
    transport_name = 'client1'
    trans = action.get_transport(transport_name, 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)
    buffer_pool = buffer_pool_lib.buffer_pool(trans)

    # loading data
    filename = '../../data/pima-indians-diabetes.csv'
    load_csv_dataset = np.genfromtxt(filename, delimiter=',')
    remote_input = remote_array(buffer_pool, input_ndarray=load_csv_dataset)
    # update context
    remote_input_metadata = remote_input.get_array_metadata()
    context_dict = {}
    context_dict["remote_input"] = remote_input_metadata
    context_dict["buffer_pool_metadata"] = buffer_pool.get_buffer_metadata()
    context_dict_in_byte = pickle.dumps(context_dict)
    params["func1"] = base64.b64encode(context_dict_in_byte).decode("ascii")
    write_params(params)

action = buffer_pool_lib.action_setup()
params = {}
main(params, action)


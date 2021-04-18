#@ type: compute
#@ parents:
#@   - func1
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma


import struct
import threading
import time
import pickle
import random
import sys
import copy
import codecs
import os
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

def main(params, action):
    # setup
    trans = action.get_transport('mem1', 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)
    context_dict_in_b64 = params["func1"]["meta"]
    context_dict = pickle.loads(base64.b64decode(context_dict_in_b64))
    buffer_pool = buffer_pool_lib.buffer_pool(trans, context_dict["buffer_pool_metadata"])
    remote_array_metadata = context_dict["remote_array"]
    remote_array_instance = remote_array(buffer_pool, metadata=remote_array_metadata)
    remote_slice_instance = remote_array_instance.get_slice(40, 180)
    remote_slice_array = remote_slice_instance.materialize()
    print(remote_slice_array)
    return {'remote array': 100}


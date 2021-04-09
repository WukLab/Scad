#@ type: compute
#@ dependents:
#@   - func2
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma
#@ import:
#@   - buffer_pool_lib.py
#@   - rdma_array.py

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

def main(params, action):
    test_data = list(range(1000))
    test_ndarray = np.array(test_data, dtype=np.int32)
    trans = action.get_transport('mem1', 'rdma')
    trans.reg(buffer_pool_lib.local_buffer_size)
    buffer_pool = buffer_pool_lib.buffer_pool(trans)
    rdma_array = remote_array(buffer_pool, input_ndarray=test_ndarray)
    context_dict = dict()
    context_dict["rdma_array"] = rdma_array.get_array_metadata()
    buffer_pool_metadata = buffer_pool.get_buffer_metadata()
    return [buffer_pool_metadata, context_dict]


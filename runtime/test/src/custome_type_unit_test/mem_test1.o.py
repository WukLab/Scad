#@ type: compute
#@ dependents:
#@   - mem_test2
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma

import pickle
import os
import datetime
import pandas as pd
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array
from disaggrt.rdma_array import init_empty_remote_array

def main(params, action):
    dt = np.dtype(np.float32)
    mem1_trans = action.get_transport("mem1", "rdma")
    mem1_trans.reg(buffer_pool_lib.buffer_size)
    trans_mapping = {"mem1": mem1_trans}
    buffer_pool = buffer_pool_lib.buffer_pool(trans_mapping)
    real_array = np.array(list(range(100, 200)), dtype = np.float32)
    empty_remote_array = init_empty_remote_array(buffer_pool, "mem1", dt, (100,))
    start_idx = 0
    end_idx = 100
    buf, buf_offset = empty_remote_array.request_mem_on_buffer_for_array(start_idx, end_idx)
    buf[buf_offset:buf_offset + 400] = real_array.tobytes()
    empty_remote_array.flush_slice(0, 100)
    context_dict = {}
    context_dict["test_array"] = empty_remote_array.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()
    context_dict_in_byte = pickle.dumps(context_dict)
    return {'meta': base64.b64encode(context_dict_in_byte).decode("ascii")}
    
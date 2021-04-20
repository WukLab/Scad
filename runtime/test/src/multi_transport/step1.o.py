#@ type: compute
#@ dependents:
#@   - step2
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma
#@   mem2:
#@     trans: mem2
#@     type: rdma
#@   mem3:
#@     trans: mem3
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


def main(params, action):
    array_slice_1 = np.array(list(range(100)), dtype=np.int32)
    array_slice_2 = np.array(list(range(100, 200)), dtype=np.int32)
    array_slice_3 = np.array(list(range(200, 300)), dtype=np.int32)
    mem1_trans = action.get_transport("mem1", "rdma")
    mem1_trans.reg(buffer_pool_lib.buffer_size)
    mem2_trans = action.get_transport("mem2", "rdma")
    mem2_trans.reg(buffer_pool_lib.buffer_size, mem1_trans)
    mem3_trans = action.get_transport("mem3", "rdma")
    mem3_trans.reg(buffer_pool_lib.buffer_size, mem1_trans)
    trans_mapping = {"mem1": mem1_trans, "mem2" : mem2_trans, "mem3" : mem3_trans}
    buffer_pool = buffer_pool_lib.buffer_pool(trans_mapping)
    remote_slice1 = remote_array(buffer_pool, input_ndarray=array_slice_1, transport_name="mem1")
    remote_slice2 = remote_array(buffer_pool, input_ndarray=array_slice_2, transport_name="mem2")
    remote_slice3 = remote_array(buffer_pool, input_ndarray=array_slice_3, transport_name="mem3")
    context_dict = {}
    context_dict["slice_1"] = remote_slice1.get_array_metadata()
    context_dict["slice_2"] = remote_slice2.get_array_metadata()
    context_dict["slice_3"] = remote_slice3.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()
    context_dict_in_byte = pickle.dumps(context_dict)
    return {'meta': base64.b64encode(context_dict_in_byte).decode("ascii")}
    
    





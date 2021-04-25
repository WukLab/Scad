#@ type: compute
#@ parents:
#@   - step1
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
import random
import numpy as np
import json
import base64
from random import randrange
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array
from disaggrt.rdma_array import merge_arrays_metadata

# debug lib
import pprint

def main(params, action):
    context_dict_in_b64 = params["step1"]['meta']
    context_dict_in_byte = base64.b64decode(context_dict_in_b64)
    context_dict = pickle.loads(context_dict_in_byte)
    mem1_trans = action.get_transport("mem1", "rdma")
    mem1_trans.reg(buffer_pool_lib.buffer_size)
    mem2_trans = action.get_transport("mem2", "rdma")
    mem2_trans.reg(buffer_pool_lib.buffer_size, mem1_trans)
    mem3_trans = action.get_transport("mem3", "rdma")
    mem3_trans.reg(buffer_pool_lib.buffer_size, mem1_trans)
    trans_mapping = {"mem1": mem1_trans, "mem2" : mem2_trans, "mem3" : mem3_trans}
    buffer_pool = buffer_pool_lib.buffer_pool(trans_mapping, context_dict["bp"])
    remote_slice1_metadata = context_dict["slice_1"]
    remote_slice2_metadata = context_dict["slice_2"]
    remote_slice3_metadata = context_dict["slice_3"]
    remote_array_merged_metadata = merge_arrays_metadata([remote_slice1_metadata, remote_slice2_metadata, remote_slice3_metadata])
    remote_array_merged = remote_array(buffer_pool, metadata=remote_array_merged_metadata)
    remote_array_slice = remote_array_merged.get_slice(5, 25)
    print(remote_array_slice.metadata.remote_mem_metadata)
    remote_array_slice_materialize = remote_array_slice.materialize()
    print(remote_array_slice_materialize)
    return {"result" : -1}
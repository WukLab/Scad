#@ type: compute
#@ parents:
#@   - mem_test1
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


def main(params, action):
    context_dict_in_b64 = params["mem_test1"]['meta']
    context_dict_in_byte = base64.b64decode(context_dict_in_b64)
    context_dict = pickle.loads(context_dict_in_byte)
    mem1_trans = action.get_transport("mem1", "rdma")
    mem1_trans.reg(buffer_pool_lib.buffer_size)
    trans_mapping = {"mem1": mem1_trans}
    buffer_pool = buffer_pool_lib.buffer_pool(trans_mapping, context_dict["bp"])
    test_array_metadata = context_dict["test_array"]
    print(test_array_metadata.remote_mem_metadata)
    test_array = remote_array(buffer_pool, metadata=test_array_metadata)
    print(test_array.materialize())
    return {"result" : -1}



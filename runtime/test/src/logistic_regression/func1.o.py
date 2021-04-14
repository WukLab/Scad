#@ type: compute
#@ dependents:
#@   - func2
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma

import pickle
import numpy as np
import json
import base64
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array
import urllib.request

def main(params, action):
    # setup
    trans = action.get_transport('mem1', 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)
    buffer_pool = buffer_pool_lib.buffer_pool(trans)

    # loading data
    csv = urllib.request.urlopen("http://172.17.0.1:8123/pima-indians-diabetes.csv")
    load_csv_dataset = np.genfromtxt(csv, delimiter=',')
    remote_input = remote_array(buffer_pool, input_ndarray=load_csv_dataset)
    # update context
    remote_input_metadata = remote_input.get_array_metadata()
    context_dict = {}
    context_dict["remote_input"] = remote_input_metadata
    context_dict["buffer_pool_metadata"] = buffer_pool.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {'meta': base64.b64encode(context_dict_in_byte).decode("ascii")}


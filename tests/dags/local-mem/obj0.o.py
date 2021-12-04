#@ type: compute
#@ withMerged:
#@   -
#@    action: ""
#@    resources:
#@      compute: 1.0
#@      memory: 1024
#@      storage: 1024
#@    elem: Compute
#@   -
#@    action: ""
#@    resources:
#@      compute: 1.0
#@      memory: 4096
#@      storage: 1024
#@    elem: Memory
#@

import numpy as np
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

def main(params, action):
    df = np.array([1,2,3])
    # build transport
    trans_name = 'memory'
    trans = action.get_transport(trans_name, 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)

    # write back
    buffer_pool = buffer_pool_lib.buffer_pool({trans_name:trans})
    rdma_array = remote_array(buffer_pool, input_ndarray=df, transport_name=trans_name)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()
    print('returning')
    return {
        'meta': context_dict,
        'result': [1, 2, 3]
    }

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
import buffer_pool_lib
from rdma_array import remote_array

# json file from front
# run in two process
# dag func1 func2 mem; transport name = mem1
# mem1 name & size 

def func1(params, action):
    test_data = list(range(1000))
    test_ndarray = np.array(test_data, dtype=np.int32)
    trans = action.get_transport(transport_name, 'rdma')
    trans.reg(buffer_pool_lib.local_buffer_size)
    buffer_pool = buffer_pool_lib.buffer_pool(trans)
    rdma_array = remote_array(buffer_pool, input_ndarray=test_ndarray)
    context_dict = dict()
    context_dict["rdma_array"] = rdma_array.get_array_metadata()
    buffer_pool_metadata = buffer_pool.get_buffer_metadata()
    return [buffer_pool_metadata, context_dict]
def func2(params, action):
    trans = action.get_transport(transport_name, 'rdma')
    buffer_pool_metadata, context_dict = params
    buffer_pool = buffer_pool_lib.buffer_pool(trans, buffer_pool_metadata)
    rdma_array = remote_array(buffer_pool, metadata=context_dict["rdma_array"])
    print(rdma_array[100])
    rdma_array[102] = 5
    print(rdma_array[102])
    print(rdma_array[905])
    print(rdma_array[799])
    
action = buffer_pool_lib.action_setup()
transport_name = 'client1'
func1_params = func1([], action)
func2(func1_params, action)
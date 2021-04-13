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
import sys
import copy
import codecs
import copyreg
import collections
import numpy as np

import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

def main(params, action):
    trans = action.get_transport('mem1', 'rdma')
    buffer_pool_metadata, context_dict = params['func1']['meta']
    buffer_pool = buffer_pool_lib.buffer_pool(trans, buffer_pool_metadata)
    rdma_array = remote_array(buffer_pool, metadata=context_dict["rdma_array"])
    print(rdma_array[100])
    rdma_array[102] = 5
    print(rdma_array[102])
    print(rdma_array[905])
    print(rdma_array[799])


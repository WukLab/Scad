#@ type: compute
#@ parallelism: 10
#@ parents:
#@   - compute1
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma

from disagg import *
import struct
import time
import numpy as np

def main(args, action):
    block_size = 1024 * 1024 * 4

    # get connection to memory object by name
    trans = action.get_transport('mem1', 'rdma')

    # register buffer for rdma
    trans.reg(block_size * 4)

    for i in range(16):
        trans.write(block_size, addr = block_size * i, offset = block_size * (i % 4))

    return {}

#@ type: compute
#@ parallelism: 10
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma

from disagg import *
import struct
import threading
import time

def main(args, action):
    int_value = 12345

    # get connection to memory object by name
    trans = action.get_transport('mem1', 'rdma')

    # register buffer for rdma
    trans.reg(1024 * 1024 * 16)

    # trans.buf is the zero-copy rdma buffer, can be accessed as normal python buffer
    # forexample, you can use pack to pack a python object into buffer
    # or you can use the buffer for binary data
    struct.pack_into('@I', trans.buf, 0, int_value)

    # write will issue an rdma request to remote
    # currently, all calls are sync. Will provide async APIs
    # address is the address for remote buffer, offset is the offset for local buffer
    # if the connection is not ready, this will be blocked
    trans.write(4, addr = 0, offset = 0)

    # read is the same
    trans.read(4, addr = 0, offset = 4)
    fetched_value = struct.unpack_from('@I', trans.buf[4:])[0]

    # verify the value
    print('API test int = {}, fetch = {}'.format(int_value, fetched_value),
            fetched_value == int_value)


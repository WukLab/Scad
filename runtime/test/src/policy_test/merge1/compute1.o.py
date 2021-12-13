#@ type: compute
#@ withMerged:
#@   -
#@     action:
#@       namespace: whisk.system
#@       name: "mem1"
#@     resources:
#@       mem: 512M
#@       cpu: 0.5
#@       storage: 64M
#@     elem: Memory
#@ limits:
#@   mem: 640M
#@   cpu: 1
#@   storage: 64M

from disagg import *
import struct
import threading
import time

batchsize = 4 * 1024 * 1024

def main(args, action):
    rv = {}

    # get connection to memory object by name
    trans = action.get_transport('mem1', 'rdma')

    # register buffer for rdma
    trans.reg(1024 * 1024 * 4)

    time.sleep(10)

    return rv


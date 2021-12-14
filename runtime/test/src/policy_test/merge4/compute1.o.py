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
import random
import time

batchsize = 4 * 1024 * 1024
iterations = 128
memsize = 512 * 1024 * 1024

def main(args, action):
    action.profile(0)
    rv = {}
    rv['start'] = time.time()
    trans = action.get_transport('mem1', 'rdma')

    # register buffer for rdma
    trans.reg(batchsize)

    for i in range(iterations):
        addr = i * batchsize % memsize
        trans.write(batchsize, addr = addr , offset = 0)
        time.sleep(random.randrange(0,50) / 1000)

    rv['end'] = time.time()
    action.profile(1)

    return rv

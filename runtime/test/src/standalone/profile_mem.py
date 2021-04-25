#@ type: compute
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma

import time

def main(_, action):
    time.sleep(1)
    action.profile(0)
    a = list(range(1024 * 1024 * 64))
    trans = action.get_transport('mem1', 'rdma')
    trans.reg(1024)
    trans.write(256, 0)
    trans.write(128, 256)
    action.profile(1)
    trans.read(128, 64)
    time.sleep(10)
    action.profile(2)


#@ type: compute
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma

import time

def main(_, action):
    time.sleep(1)

    action.profile(0)
    # 4K pages!
    trans = action.get_transport('mem1', 'rdma')
    trans.reg(4096)

    # trans2 = action.get_transport('mem2', 'rdma')
    # trans2.reg(4096)

    a = list(range(1024 * 1024 * 64))
    trans.write(256, 0)
    trans.write(128, 256)
    # trans2.read(64, 128)

    action.profile(1)
    trans.read(64, 128)
    # trans2.write(128, 256)
    time.sleep(10)

    action.profile(2)


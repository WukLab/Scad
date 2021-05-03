#@ type: compute
#@ parents:
#@   - setup
#@ dependents:
#@   - final
#@ parallelism: 16
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma

import time

def main(_, action):
    
    bufBytes = 1024 * 1024 * 128

    trans = action.get_transport('mem1', 'rdma')
    trans.reg(bufBytes)

    rv = {}

    # tput
    rv['tput'] = {}
    reqBytes = 1024 * 1024 * 2

    t0 = time.time()
    for i in range(0, bufBytes, reqBytes):
        trans.read(reqBytes, i, i)
    t1 = time.time()
    rv['tput']['rd'] = bufBytes / 1024 / 1024 / (t1 - t0)

    t0 = time.time()
    for i in range(0, bufBytes, reqBytes):
        trans.write(reqBytes, i, i)
    t1 = time.time()
    rv['tput']['wr'] = bufBytes / 1024 / 1024 / (t1 - t0)
    
    # latency
    accesses = 64
    lats = {}
    for reqBytes in [1024 * 2 ** n for n in range(1, 12)]:
        lats[reqBytes] = {}
        t0 = time.time()
        for i in range(accesses):
            trans.read(reqBytes, i * reqBytes)
        t1 = time.time()
        lats[reqBytes]['rd'] = (t1 - t0) / accesses

        lats[reqBytes] = {}
        t0 = time.time()
        for i in range(accesses):
            trans.write(reqBytes, i * reqBytes)
        t1 = time.time()
        lats[reqBytes]['wr'] = (t1 - t0) / accesses

    rv['lat'] = lats
    print(rv)
    return rv
            

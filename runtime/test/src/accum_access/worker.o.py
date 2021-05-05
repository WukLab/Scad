#@ type: compute
#@ parents:
#@   - setup
#@ dependents:
#@   - final
#@ parallelism: 4
#@ corunning:
#@   mem1:
#@     trans: mem1
#@     type: rdma
#@ limits:
#@    mem: 512 MB

import time

def main(_, action):
    
    bufBytes = 1024 * 1024 * 64
    num_loops = 32

    trans = action.get_transport('mem1', 'rdma')
    trans.reg(bufBytes)

    rv = {}

    # tput
    rv['tput'] = {}
    reqBytes = 1024 * 1024 * 2

    t0 = time.time()
    for i in range(num_loops):
        for i in range(0, bufBytes, reqBytes):
            trans.read(reqBytes, i, i)
    t1 = time.time()
    rv['tput']['rd'] = num_loops * bufBytes / 1024 / 1024 / (t1 - t0)

    t0 = time.time()
    for i in range(num_loops):
        for i in range(0, bufBytes, reqBytes):
            trans.write(reqBytes, i, i)
    t1 = time.time()
    rv['tput']['wr'] = num_loops * bufBytes / 1024 / 1024 / (t1 - t0)

    time.sleep(10)
    
    # latency
    accesses = 64
    lats = {}
    for reqBytes in [1024 * 2 ** n for n in range(1, 11)]:
        lats[reqBytes] = {}
        t0 = time.time()
        for i in range(accesses):
            trans.read(reqBytes, i * reqBytes)
        t1 = time.time()
        lats[reqBytes]['rd'] = (t1 - t0) / accesses

        t0 = time.time()
        for i in range(accesses):
            trans.write(reqBytes, i * reqBytes)
        t1 = time.time()
        lats[reqBytes]['wr'] = (t1 - t0) / accesses

    rv['lat'] = lats
    print(rv)
    return rv
            

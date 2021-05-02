#@ type: compute
#@ parents:
#@   - obj0
#@ corunning:
#@   - obj1
#@ dependents:
#@   - obj3
#@ parallelism: 4

import time

def main(params, action):
    time.sleep(0)
    return params
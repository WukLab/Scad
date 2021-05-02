#@ type: compute
#@ parents:
#@   - obj0
#@ dependents:
#@   - obj3
#@ corunning:
#@   - obj2

import time

def main(params, action):
    time.sleep(0)
    return params
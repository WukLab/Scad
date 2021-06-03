#@ type: compute
#@ parents:
#@   - obj0
#@ dependents:
#@   - obj2
#@ parallelism: 8

import time

def main(params, action):
    time.sleep(1)
    return {"index": "1"}
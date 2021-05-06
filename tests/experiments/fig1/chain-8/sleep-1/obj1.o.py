#@ type: compute
#@ parents:
#@   - obj0
#@ dependents:
#@   - obj2

import time

def main(params, action):
    time.sleep(.001)
    return {"index": "1"}
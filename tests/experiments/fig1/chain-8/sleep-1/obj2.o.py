#@ type: compute
#@ parents:
#@   - obj1
#@ dependents:
#@   - obj3

import time

def main(params, action):
    time.sleep(.001)
    return {"index": "1"}

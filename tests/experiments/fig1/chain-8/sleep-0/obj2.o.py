#@ type: compute
#@ parents:
#@   - obj1
#@ dependents:
#@   - obj3

import time

def main(params, action):
    time.sleep(0)
    return {"index": "1"}
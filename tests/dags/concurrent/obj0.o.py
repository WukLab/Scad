#@ type: compute
#@ dependents:
#@   - obj1
#@   - obj2

import time

def main(params, action):
    time.sleep(0)
    return {"aaaaaa": 123123}
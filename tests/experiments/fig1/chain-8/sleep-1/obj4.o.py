#@ type: compute
#@ parents:
#@   - obj3
#@ dependents:
#@   - obj5
import time

def main(params, action):
    time.sleep(.001)
    return {"index": "1"}
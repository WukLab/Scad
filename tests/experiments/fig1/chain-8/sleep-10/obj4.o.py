#@ type: compute
#@ parents:
#@   - obj3
#@ dependents:
#@   - obj5
import time

def main(params, action):
    time.sleep(.01)
    return {"index": "1"}
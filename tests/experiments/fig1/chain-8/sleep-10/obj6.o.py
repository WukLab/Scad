#@ type: compute
#@ parents:
#@   - obj5
#@ dependents:
#@   - obj7
import time

def main(params, action):
    time.sleep(.01)
    return {"index": "1"}

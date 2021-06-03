#@ type: compute
#@ parents:
#@   - obj2
#@ dependents:
#@   - obj4
import time

def main(params, action):
    time.sleep(0)
    return {"index": "1"}

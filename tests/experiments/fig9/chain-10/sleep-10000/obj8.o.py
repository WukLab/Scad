#@ type: compute
#@ parents:
#@   - obj7
#@ dependents:
#@   - obj9
import time

def main(params, action):
    time.sleep(10)
    return {"index": "1"}

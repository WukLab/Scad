#@ type: compute
#@ parents:
#@   - obj5
#@ dependents:
#@   - obj7
import time

def main(params, action):
    time.sleep(10)
    return {"index": "1"}

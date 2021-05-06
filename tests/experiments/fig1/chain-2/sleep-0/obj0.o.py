#@ type: compute
#@ dependents:
#@   - obj1
import time

def main(params, action):
    time.sleep(0)
    return {"index": "1"}
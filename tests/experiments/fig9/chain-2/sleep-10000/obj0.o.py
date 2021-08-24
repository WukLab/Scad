#@ type: compute
#@ dependents:
#@   - obj1
import time

def main(params, action):
    time.sleep(10)
    return {"index": "1"}

#@ type: compute
#@ parents:
#@   - obj4
#@ dependents:
#@   - obj6
import time

def main(params, action):
    time.sleep(.1)
    return {"index": "1"}

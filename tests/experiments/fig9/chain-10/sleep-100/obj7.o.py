#@ type: compute
#@ parents:
#@   - obj6
#@ dependents:
#@   - obj8
import time

def main(params, action):
    time.sleep(.1)
    return {"index": "1"}

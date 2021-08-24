#@ type: compute
#@ parents:
#@   - obj8

import time

def main(params, action):
    time.sleep(10)
    return {"index": "9-1000"}

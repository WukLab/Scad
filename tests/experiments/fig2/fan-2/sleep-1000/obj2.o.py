#@ type: compute
#@ parents:
#@   - obj1

import time

def main(params, action):
    time.sleep(1)
    return {"index": "1"}
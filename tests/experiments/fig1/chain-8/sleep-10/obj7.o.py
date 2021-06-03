#@ type: compute
#@ parents:
#@   - obj6

import time

def main(params, action):
    time.sleep(.01)
    return {"index": "1"}

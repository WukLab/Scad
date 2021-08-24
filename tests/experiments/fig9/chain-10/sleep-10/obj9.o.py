#@ type: compute
#@ parents:
#@   - obj8

import time

def main(params, action):
    time.sleep(.01)
    return {"index": "9-10"}

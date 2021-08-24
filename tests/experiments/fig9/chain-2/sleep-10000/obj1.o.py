#@ type: compute
#@ parents:
#@   - obj0
import time

def main(params, action):
    time.sleep(10)
    return {"index": "1"}

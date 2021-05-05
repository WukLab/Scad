#@ type: compute
#@ dependents:
#@   - worker
#@   - mem1

import time

def main(_, action):
    time.sleep(10)
    return {}
            

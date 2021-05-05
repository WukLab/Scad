#@ type: compute

import requests
import os
import json
import time

# SET __OW_ACTION_NAME
# SET __OW_ACTION_VERSION
# SET __OW_APP_ACTIVATION_ID
# SET __OW_DEADLINE
# SET __OW_FUNCTION_ACTIVATION_ID
# SET __OW_NAME
# SET __OW_NAMESPACE
# SET __OW_SERVER_URL
# SET __OW_TRANSACTION_ID

# /**
#  * An object representing a request to schedule a new swap object
#  *
#  * @param originalAction the fully qualified name of the object requesting the swap space. e.g. {namespace}/action
#  * @param source the invoker where the swap request is coming from
#  * @param functionActivationId the activation ID belonging to the function which is requesting this swap
#  * @param appActivationId the application ID of the application requesting this swap
#  *

host = "http://172.17.0.1:3233"
auth = ("789c46b1-71f6-4ed5-8c54-816aa4f8c502", "abczO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP")

class Swap:
    def __init__(self, action):
        self.count = 0
        self.name = os.environ['__OW_ACTION_NAME']
        self.invoker = os.environ['__OW_INVOKER_ID']
        self.aid_app = os.environ['__OW_APP_ACTIVATION_ID']
        self.aid_func = os.environ['__OW_FUNCTION_ACTIVATION_ID']
        self.url = "{}/api/v1/{}/swap/{}".format(
            host, os.environ['__OW_NAMESPACE'], os.environ['__OW_NAME'])

        self.action = action
        self.trans = []

    def postRequest(self, size):
        data = { 'originalAction':          self.name,
                 'invoker':                 self.invoker,
                 'functionActivationId':    self.aid_func,
                 'appActivationId':         self.aid_app,
                 'mem':                     '{} MB'.format(size) }
        print('sending request to {} with data'.format(self.url), data)
        r = requests.post(self.url, json = data, auth=auth)
        res = r.json()
        return res

def main(_, action):
    print('makeing the swap call')
    s = Swap(action)
    r = s.postRequest(128)
    print(r)
    return {'res': r}
    

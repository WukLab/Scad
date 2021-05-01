import requests
import threading
import json

INVOKER_API_PORT = 8234

class AddressBook:
    def __init__(self):
        self.mutex = threading.Lock()
        # Either [(InvokerId, Request)] AddressBook
        self.addressbook = dict()
        pass

    def _dict_key(self, request):
        return (request.activationId, request.name)

    def _reply_with(self, request, ip, address):
        url = "http://{}:{}/address/signal"
        data = json.dumps({'request': request, 'address': address})
        requests.post(url, data=json.dumps(data))

    def postWait(self, invokerId, reqIp, request):
        key = self._dict_key(request)
        self.mutex.acquire()

        isSignaled, value = self.addressBook.get(key, (False, []))
        if isSignaled:
            self.mutex.release()
            return value
        else:
            reqTuple = (invokerId, reqIp, request)
            self.addressBook[key] = True, value + [reqTuple]
            self.mutex.release()
            return None

    def signalReady(self, invokerId, address):
        key = self._dict_key(address)
        self.mutex.acquire()
        # broadcast requests
        isSignaled, reqList = self.addressBook.get(key, (False, []))
        if isSignaled:
            self.mutex.release()
            return
        
        self.addressBook[key] = True, address
        self.mutex.release()

        toSignal = [t for t in reqList if t[0] != invokerId]
        for _, ip, request in toSignal:
            self._reply_with(reqInvoker, request, ip, address)
        

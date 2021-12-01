from multiprocessing import Pipe, Process
from disagg import LibdAction
from libdruntime import LibdServices

# ========================
# Start of Tempalte
{% for main in mains %}
  {{ main.username }}
{% endfor %}

mains = {{ main_names }}
transports = {{ transports }}
# End of Jinjia Template
# ========================

pipes = []
executors = []

class RuntimeSingle(LibdServices):
    def __init__(self, aid, pipe, args):
        self.action = LibdAction(self.cv, aid, **args)
        self.action.runtime = self

        self.pipe = pipe
    def terminate(self):
        self.action.terminate()
    def handler(self):
        def _handle():
            while True:
                cmd, params, body = self.pipe.recv()
                # TODO: sync with launcher.py
                if cmd == 'ACTMSGS':
                    msgs = {}
                    for name, t in self.action.raw_transports.items():
                        size, msg = t.get_msg()
                        if size > 0:
                            msgs[name] = msg
                    self.pipe.send(msgs)
                elif cmd == 'TRANSADD':
                    self.action.add_transport(body['durl'])
                elif cmd == 'TRANSCONF':
                    self.action.config_transport(params[1], body['durl'])

        return _handle

def main_wrapper(payload, main, pipe, aid, mainargs):
    # create runtime and actions
    runtime = RuntimeSingle(aid, mainargs)
    # start runtime for handlign messages
    threading.Thread(target=runtime.handler()).start()

    ret = main(payload, runtime.action)

    runtime.terminate()
    pipe.send(ret)

actions = []
def _handle_msgs(sendQ, recvQ):
    while True:
        msg = recvQ.get()
        cmd, params, body = msg
        name = None
        if cmd == 'ACTMSGS':
            # pull all messages
            messages = {}
            for p in pipes():
                p.send(msg)
            for p in pipes():
                messages.update(p.recv())
            sendQ.put(messages)
        else:
            # do not need reply, but need dispatch to main
            name = None
            if cmd == 'TRANSADD':
                name = body['durl'].split(';')[0]
            elif cmd == 'TRANSCONF':
                name = params[1]

            # forward the message
            for i, ts in enumerate(transports):
                if name in ts:
                    pipes[i].send(msg)

def main(payload, conn):
    # mini runtime
    sendQ, recvQ = conn
    msg, params, body = recvQ.get()
    if msg != 'ACTADD':
        return False
    # create wrappers
    aid, args = params
    for m in mains:
        mp, sp = Pipe()
        sub = Process(target=main_wrapper, args=(payload,m,sp,aid,args))
        executors.append(sub)
        pipes.append(mp)

    # dispatch messages
    threading.Thread(target=_handle_msgs, args=(sendQ, recvQ)).start()

    # join
    for p in executors:
        p.join()
    res = []
    for p in pipes:
        res.append(p.recv())
    # result will be a queue
    return res


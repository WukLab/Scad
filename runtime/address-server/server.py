from flask import Flask, request
from addressbook import AddressBook

app = Flask(__name__)
book = AddressBook()

@app.route('/<invokerId>/post')
def postWait(invokerId):
    reqs = request.get_json()
    res = [book.postWait(invokerId, request.remote_addr, r) for r in reqs]
    return {
        'status': 'ok',
        'res': res
    }

@app.route('/<invokerId>/signal')
def signalReady(invokerId):
    for addr in request.get_json():
        book.signalReady(invokerId, addr)
    return { 'status': 'ok' }


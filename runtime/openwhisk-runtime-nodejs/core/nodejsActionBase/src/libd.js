const ref = require('ref-napi');
const ffi = require('ffi-napi');
const fetch = require('node-fetch');

let voidT = ref.types.void
let voidP = ref.refType(voidT)
let actionP = ref.refType(voidT)
let transportP = ref.refType(voidT)

// FFI interface to libd
let libd = ffi.Library('libd', {
  'libd_action_init': [ actionP, [ 'string', 'string' ] ],
  'libd_action_free': [ 'int', [ actionP ] ],

  'libd_action_add_transport':    [ transportP, [ actionP, 'string' ] ],
  'libd_action_config_transport': [ 'int', [ transportP, 'string', 'string' ] ],
  'libd_action_get_transport':    [ transportP, [ actionP, 'string' ] ],

  // transport statemachines
  'libd_transport_init':       [ 'int', [ transportP ] ],
  'libd_transport_connect':    [ 'int', [ transportP ] ],
  'libd_transport_recover':    [ 'int', [ transportP ] ],
  'libd_transport_terminate':  [ 'int', [ transportP ] ],

  // User APIs, rdma
  'libd_trdma_read':  [ 'int', [ transportP, 'long', 'long', voidP ] ],
  'libd_trdma_write': [ 'int', [ transportP, 'long', 'long', voidP ] ],

  // User APIs, server
  'libd_trdma_server_serve': [ 'int', [ transportP ] ],
});

// Interface for web request
class LibdRequest {
    constructor(serverUrl, actionId) {
        this.serverUrl = serverUrl
        this.actionId = actionId
    }

    async dependency(target = null, value = null, parallelism = null, dependency = null,
        functionActivationId = null, appActivationId = null) {
        let path = `${this.serverUrl}/activation/${this.actionId}/dependency`
        let requestPath = target || process.env['__OW_ACTION_NAME']
        let json = { target: requestPath }
        
        json['functionActivationId'] = functionActivationId || process.env['__OW_FUNCTION_ACTIVATION_ID']
        json['appActivationId']      = appActivationId || process.env['__OW_APP_ACTIVATION_ID']
        if (value)                json['value'] = value
        if (parallelism)          json['parallelism'] = parallelism
        if (dependency)           json['dependency'] = dependency

        let params = JSON.stringify(json)

        console.log(`call invocation ${path} with parameter ${params}`)
        fetch(path, {
            method: 'post',
            body:    params,
            headers: { 'Content-Type': 'application/json' },
        }).then(res => console.log(`request dependency ${json} with return ${res}`))
    }


}

// Wrapper for c libs
class LibdAction {

    constructor(actionId, serverUrl) {
        this.actionId = actionId
        this.serverUrl = serverUrl
        this.action = libd.libd_action_init(actionId, serverUrl)
        this.request = new LibdRequest(serverUrl, actionId)
        console.log(`action init with ${this.actionId} ${this.serverUrl} ${this.action}`)
    }

    init_transport(durl) {
        console.log(`call init transport ${durl}, with ${this.actionId}`)
        return libd.libd_action_add_transport(this.action, durl)
    }

    config_transport(name, durl) {
        return libd.libd_action_config_transport(this.action, name, durl)
    }

    get_transport(name, type) {
        let trans = libd.libd_action_get_transport(this.action, name)
        switch (type) {
            case 'rdma':        return new LibdRDMA(trans)
            case 'rdma_server': return new LibdRDMAServer(trans)
            default:            return null
        }
    }
}

class LibdRDMAServer {

    constructor(trans) {
        this.transport = trans
    }

    serve() {
        // TODO: change this to async
        libd.libd_trdma_server_serve(this.transport)
    }

}
class LibdRDMA {

    constructor(trans) {
        this.transport = trans
    }

    alloc(size) {
        return Buffer.allocUnsafe(size)
    }

    read(address, size, buffer) {
        return libd.libd_trdma_read(this.transport, address, size, buffer)
    }

    write(address, size, buffer) {
        return libd.libd_trdma_write(this.transport, address, size, buffer)
    }
}

module.exports = { LibdAction }

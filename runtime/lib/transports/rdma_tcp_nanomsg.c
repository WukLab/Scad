#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>

#include "libd.h"
#include "libd_transport.h"
#include "libd_trdma.h"
#include "transports/rdma_tcp.h"

static int _request (int sock, uint64_t addr, uint64_t req_size, int op, void *data)
{
    struct tcp_rdma_cmd *cmd = NULL;
    int bytes = -1, ret = -1;

    int cmd_size = sizeof(struct tcp_rdma_cmd);
    int send_size = op == TCP_RDMA_OP_WRITE ? cmd_size + req_size : cmd_size;
    int recv_size = op == TCP_RDMA_OP_WRITE ? cmd_size : cmd_size + req_size;
    cmd = (struct tcp_rdma_cmd *)malloc(cmd_size + req_size);

    // request...
    cmd->op = op;
    cmd->addr = addr;
    cmd->size = req_size;
    cmd->status = 0;
    if (op == TCP_RDMA_OP_WRITE)
        memcpy(cmd->data, data, req_size);

    if ((bytes = nn_send(sock, cmd, send_size, 0)) < 0) {
        ret = -1;
        goto cleanup;
    }

    // wait for request..
    if ((bytes = nn_recv(sock, cmd, recv_size, 0)) < 0) {
        ret = -1;
        goto cleanup;
    }

    if (cmd->status == 0 && op == TCP_RDMA_OP_READ)
        memcpy(data, cmd->data, req_size);
    ret = cmd->status;

cleanup:
    free(cmd);
    return ret;
}

// interface implementations
static int _init(struct libd_transport *trans) {
    init_config_for(trans, struct tcp_rdma_state);

    dprintf("start setup for %s", trans->tstate->name);

    init_config_require(url, id);
    init_config_set(sock, 0);
    init_config_set(sock_initd, 0);

    get_local_state(rstate,trans,struct tcp_rdma_state);
    dprintf("setup for %s: size %d, url %s",
        trans->tstate->name, rstate->size, rstate->url);

    // init socket
    if ((rstate->sock = nn_socket(AF_SP, NN_REQ)) < 0)
        return -1;
    rstate->sock_initd = 1;

    return 0;
}

static int _connect(struct libd_transport *trans) {
    int rv;
    get_local_state(rstate,trans,struct tcp_rdma_state);

    if ((rv = nn_connect (rstate->sock, rstate->url)) < 0) {
        return rv;
    }
    dprintf("Setup connection rv %d", rv);
        
    return 0;
}

static int _terminate(struct libd_transport * trans) {
    get_local_state(rstate,trans,struct tcp_rdma_state);
    // TODO: cleanup state
    return (nn_shutdown(rstate->sock, 0));
}

// actions
static void * _reg(struct libd_transport *trans, size_t s, void *buf) {
    return NULL;
}

static int _read(struct libd_transport *trans,
                size_t size, uint64_t addr, void * buf) {
    get_local_state(rstate,trans,struct tcp_rdma_state);
    return _request(rstate->sock, addr, size, TCP_RDMA_OP_READ, buf);
}

static int _write(struct libd_transport *trans,
                  size_t size, uint64_t addr, void * buf) {
    get_local_state(rstate,trans,struct tcp_rdma_state);
    return _request(rstate->sock, addr, size, TCP_RDMA_OP_WRITE, buf);
}

// export struct
struct libd_trdma rdma_tcp = {
    .trans = {
        .init = _init,
        .terminate = _terminate,
        .connect = _connect
    },

    .reg = _reg,
    .read = _read,
    .write = _write
};


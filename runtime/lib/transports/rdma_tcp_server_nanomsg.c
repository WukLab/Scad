#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>

#include "libd.h"
#include "libd_transport.h"
#include "interfaces/libd_trdma_server.h"
#include "transports/rdma_tcp.h"

// interface implementations
static int _init(struct libd_transport *trans) {
    dprintf("server init");

    init_config_for(trans, struct tcp_rdma_state);

    init_config_require(size, atoi);
    init_config_require(url, id);
    init_config_set(sock, 0);
    init_config_set(sock_initd, 0);
    init_config_set(mr, NULL);

    get_local_state(rstate,trans,struct tcp_rdma_state);
    // init sock
    if ((rstate->sock = nn_socket(AF_SP, NN_REP)) < 0) {
        dprintf("nanomsg sock init fail with rv %d", rstate->sock);
        return rstate->sock;
    }
    rstate->sock_initd = 1;
    // setup mr
    rstate->mr = malloc(rstate->size);

    return 0;
}

static int _connect(struct libd_transport *trans) {
    int rv;
    dprintf("server connect");
    get_local_state(rstate,trans,struct tcp_rdma_state);

    if ((rv = nn_bind (rstate->sock, rstate->url)) < 0) {
        dprintf("Server error at binding");
        return -rv;
    }

    return 0;
}

static int _terminate(struct libd_transport * trans) {
    get_local_state(rstate,trans,struct tcp_rdma_state);
    // TODO: cleanup state
    return (nn_shutdown(rstate->sock, 0));
}

static int _serve(struct libd_transport *trans) {
    get_local_state(rstate,trans,struct tcp_rdma_state);
    struct tcp_rdma_cmd *req = NULL, *rep;
    int cmd_size = sizeof(struct tcp_rdma_cmd), send_size, bytes;

    dprintf("start servering at %s", rstate->url);
    for (;;) {
		if ((bytes = nn_recv(rstate->sock, &req, NN_MSG, 0)) < 0) {
            // TODO: better error function
            trans->tstate->state = LIBD_TRANS_STATE_ERROR;
            return bytes;
		}

        dprintf("get a message of size %d", bytes);
        if (req->op == TCP_RDMA_OP_READ) {
            send_size = cmd_size + req->size;
            rep = (struct tcp_rdma_cmd *)malloc(cmd_size + req->size);
            rep->op = TCP_RDMA_OP_READ;
            rep->size = req->size;
            rep->status = 0;

            memcpy(rep->data, (char*)rstate->mr + req->addr, req->size);
        } else { // if (req->op == TCP_RDMA_OP_WRITE)
            send_size = cmd_size;
            rep = (struct tcp_rdma_cmd *)malloc(cmd_size);
            rep->op = TCP_RDMA_OP_WRITE;
            rep->size = req->size;
            rep->status = 0;

            memcpy((char*)rstate->mr + req->addr, req->data, req->size);
        }

        nn_freemsg(req);

        if ((bytes = nn_send(rstate->sock, rep, send_size, 0)) < 0) {
            trans->tstate->state = LIBD_TRANS_STATE_ERROR;
            return bytes;
        }
    }
}

struct libd_trdma_server rdma_tcp_server = {
    .trans = {
        .init = _init,
        .terminate = _terminate,
        .connect = _connect
    },

    .serve = _serve
};


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>

#include "libd.h"
#include "libd_transport.h"
#include "interfaces/libd_trdma_server.h"
#include "transports/rdma_uverbs.h"

// create a static protected domain that shares among all buffers
static struct ibv_pd      *_pd      = NULL;
static struct ibv_context *_context = NULL;

// Network
static void fatal(const char *func)
{
        fprintf(stderr, "%s: %s\n", func, nn_strerror(nn_errno()));
        exit(1);
}

static int server_exchange_info(struct rdma_conn *conn, const char * url) {
	static int sock, sock_initd = 0;
    int rv;
    int send_size, bytes;
    void *info, *peerinfo = NULL;

    if (!sock_initd) {
        if ((sock = nn_socket(AF_SP, NN_REP)) < 0) {
            fatal("nn_socket");
        }

        if ((rv = nn_bind(sock, url)) < 0) {
            fatal("nn_bind");
        }
        sock_initd = 1;
    }

    if ((bytes = nn_recv(sock, &peerinfo, NN_MSG, 0)) < 0) {
        fatal("nn_recv");
    }

    // TODO: fix this, use memcpy
    conn->peerinfo = peerinfo;
    send_size = extract_info(conn, &info);
    if ((bytes = nn_send(sock, info, send_size, 0)) < 0) {
        fatal("nn_send");
    }
    dprintf("send %d bytes, expect to send %d bytes", send_size, bytes);

    // TODO: check this
    // nn_freemsg(buf);
    // nn_shutdown(sock, 0);

    return 0;
}

// interface implementations
// init might be called multiple times
static int _init(struct libd_transport *trans) {
    dprintf("server init");
    init_config_for(trans, struct uverbs_rdma_state);
    get_local_state(rstate,trans,struct uverbs_rdma_state);

    init_config_set(device_name, "mlx5_1");
    init_config_set(num_devices, 2);
    init_config_set(cq_size, 16);

    rstate->num_conns = 0;
    rstate->conns = NULL;

    memset(&rstate->conn, 0, sizeof(struct rdma_conn));
    rstate->conn.gid = 0;
    rstate->conn.port = 1;

    // init using the global context and PD
    if (_context == NULL)
        _context = create_context(rstate->num_devices, rstate->device_name);
    rstate->conn.context = _context;

    if (_pd == NULL)
        _pd = ibv_alloc_pd(rstate->conn.context);
    rstate->conn.pd = _pd;

    // String to size_t
    init_config_require(size, config_to_ull);
    init_config_require(url,  id);

    // create MRs
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
	create_mr(&rstate->conn, rstate->size, access, NULL);

    dprintf("setup for %s: size %ld, url %s",
        trans->tstate->name, rstate->size, rstate->url);

    return 0;
}

static int _connect(struct libd_transport *trans) {
    // get_local_state(rstate,trans,struct uverbs_rdma_state);

	// For now, we create server at _serve call

    // if ((rv = nn_bind (rstate->sock, rstate->url)) < 0) {
    //     dprintf("Server error at binding");
    //     return -rv;
    // }

    return 0;
}

static int _terminate(struct libd_transport * trans) {
    get_local_state(rstate,trans,struct uverbs_rdma_state);

    // clean up MR
    for (int i = 0; i < rstate->conn.num_mr; i++) {
        void * buf = rstate->conn.mr[i].addr;
        ibv_dereg_mr(rstate->conn.mr + i);
        free(buf);
    }

    // clean up nanomsg
    for (int i = 0; i < rstate->num_conns; i++) {
        struct rdma_conn *conn = rstate->conns[i];
        if (conn->peerinfo != NULL)
            nn_freemsg(conn->peerinfo);

        // TODO: close qp and cq, instead of context
        ibv_destroy_qp(conn->qp);
        ibv_destroy_cq(conn->cq);

        free(conn);
    }
    free(rstate->conns);
    ibv_close_device(_context);

    return 0;
}

static int _serve(struct libd_transport *trans) {
    get_local_state(rstate,trans,struct uverbs_rdma_state);
    // Exchange with server

    for (;;) {
        struct rdma_conn *conn = 
            (struct rdma_conn *)calloc(1, sizeof(struct rdma_conn));

        // save conn to conns
        rstate->conns = realloc(rstate->conns,
            (rstate->num_conns + 1) * sizeof(void *));
        rstate->conns[rstate->num_conns] = conn;
        ++ (rstate->num_conns);

        conn->num_mr = rstate->conn.num_mr;
        conn->mr     = rstate->conn.mr;
        conn->gid    = 0;
        conn->port   = 1;
        conn->context = _context;
        conn->pd     = _pd;

        // setup mr
        conn->cq = ibv_create_cq(_context, rstate->cq_size, NULL, NULL, 0);
        create_qp(conn);
        dprintf("server listening on %s...", rstate->url);
        server_exchange_info(conn, rstate->url);

        // Enable QP, server only need to get to RTR
        qp_stm_reset_to_init(conn);
        qp_stm_init_to_rtr(conn);
    }
}

struct libd_trdma_server rdma_uverbs_server = {
    .trans = {
        .init = _init,
        .terminate = _terminate,
        .connect = _connect
    },

    .serve = _serve
};


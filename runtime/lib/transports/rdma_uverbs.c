#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <nanomsg.h>
#include <infiniband/verbs.h>

#include "libd.h"
#include "libd_transport.h"
#include "libd_trdma.h"
#include "transports/rdma_tcp.h"

// create a static protected domain that shares among all buffers
static struct ibv_pd      *_pd      = NULL;
static struct ibv_context *_context = NULL;

// Network
static void fatal(const char *func)
{
        fprintf(stderr, "%s: %s\n", func, nn_strerror(nn_errno()));
        exit(1);
}

static int client_exchange_info(struct uverbs_rdma_state *state) {
    int sock, rv;
    int send_size, bytes;
    void *info, *peerinfo = NULL;
    int size = 1024 * 64;

    // init nanomsg socket
    printf("connecting to server %s...\n", state->url);
    if ((sock = nn_socket(AF_SP, NN_REQ)) < 0) {
        fatal("nn_socket");
    }

    if (nn_setsockopt(sock, NN_SOL_SOCKET, NN_RCVBUF, &size, sizeof(size)) != 0) {
        fatal("nng_setopt_size");
    }
    if (nn_setsockopt(sock, NN_SOL_SOCKET, NN_SNDBUF, &size, sizeof(size)) != 0) {
        fatal("nng_setopt_size");
    }
    size = -1;
    if (nn_setsockopt(sock, NN_SOL_SOCKET, NN_RCVMAXSIZE, &size, sizeof(size)) != 0) {
        fatal("nng_setopt_size");
    }

    if ((rv = nn_connect(sock, config.server_url)) < 0) {
        fatal("nn_connect");
    }

    send_size = extract_info(conn, &info);
    printf("try to send size %d\n", send_size);
    if ((bytes = nn_send(sock, info, send_size, 0)) < 0) {
        fatal("nn_send");
    }
    if ((bytes = nn_recv(sock, &peerinfo, NN_MSG, 0)) < 0) {
        fatal("nn_recv");
    }

    conn->peerinfo = peerinfo;
    printf("received mr, with bytes %d, num %d\n", bytes, conn->peerinfo->num_mr);

    // TODO: check this
    // nn_freemsg(buf);

    free(info);
    return 0;
}

// TODO: async requests
static inline int _request(struct ibv_qp * conn, size_t size, int opcode,
                uint64_t local_addr, uint64_t remote_addr) {
    int ret;
    struct ibv_wc wc;
    int bytes;

    // SGE for request, we use only 1
    struct ibv_sge sge;
    sge.addr = local_addr;
    sge.length = size;
    sge.lkey = lkey;

    struct ibv_send_wr wr, *badwr = NULL;

    wr.wr_id = 233;
    wr.sg_list = sge;
    wr.num_sge = 1;

    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;
    wr.opcode = opcode;

    wr.send_flags = IBV_SEND_SIGNALED;
    wr.next = NULL;

    // Send and poll
	ibv_post_send(conn->qp, &wr, &badwr);
	while ((bytes = ibv_poll_cq(conn->cq, 1, &wc)) == 0) ;
	return 0;
}

// TODO: change the order of init.
// interface implementations
static int _init(struct libd_transport *trans) {
    init_config_for(trans, struct tcp_rdma_state);

    debug_map_print(trans->tstate->config);

    // first, setup rdma-related stuff; wait for run command
    get_local_state(rstate,trans,struct tcp_rdma_state);
    dprintf("setup for %s: size %d, url %s",
        trans->tstate->name, rstate->size, rstate->url);

    // init the RDMA connection
    memset(&rstate->conn, 0, sizeof(struct rdma_conn));
    // init using the global context and PD
    if (_context == NULL)
        _context = create_context(rstate->num_devices, rstate->device);
    rstate->conn.context = _context;

    if (_pd == NULL)
        _pd = ibv_alloc_pd(rstate->conn.context);
    rstate->conn.pd = _pd;

    if (rstate->conn.pd == NULL) {
        printf("create pd fail");
        return -1;
    }

    rstate->conn.cq = ibv_create_cq(
        rstate->conn.context, rstate->cq_size, NULL, NULL, 0);

    // Create QP
    create_qp(&rstate->conn);

    // set rdma un-related parameter
    init_config_set(num_devices, 2);
    init_config_set(device, "mlx5_1");
    init_config_require(url, id);

    return 0;
}

static int _connect(struct libd_transport *trans) {
    get_local_state(rstate,trans,struct tcp_rdma_state);

    // Exchange with server
    client_exchange_info(rstate);

    // moving the stats of QP
    qp_stm_reset_to_init (&rstate->conn);
    qp_stm_init_to_rtr   (&rstate->conn);
    qp_stm_rtr_to_rts    (&rstate->conn);

    // Connect to server
    if ((rv = nn_connect (rstate->sock, rstate->url)) < 0) {
        return rv;
    }
    dprintf("Setup connection rv %d", rv);
        
    return 0;
}

static int _terminate(struct libd_transport * trans) {
    get_local_state(rstate,trans,struct uverbs_rdma_state);
    // TODO: cleanup local data structure

    // clean up
    for (int i = 0; i < conn.num_mr; i++) {
        ibv_dereg_mr(conn.mr + i);
        // Free MR
    }
    ibv_close_device(conn.context);

    // TODO: clean up the QP
    return (nn_shutdown(rstate->sock, 0));

    return 0;
}

// actions
static void * _reg(struct libd_transport *trans, size_t s, void *buf) {
    get_local_state(rstate,trans,struct tcp_rdma_state);

    if (buf == NULL) {
        // This buffer will be fine if its in the same PD
        buf = create_mr(&rstate->conn, s, IBV_ACCESS_LOCAL_WRITE);
    }
    return buf;
}

static int _read(struct libd_transport *trans,
                size_t size, uint64_t addr, void * buf) {
    get_local_state(rstate,trans,struct tcp_rdma_state);
static inline int _request(struct ibv_qp * qp, size_t size, int opcode,
                int lkey, uint64_t local_addr,
				int rkey, uint64_t remote_addr) {
    return _request(rstate->conn, size, IBV_WR_RDMA_READ,
                    (uint64_t)buf, addr);
}

static int _write(struct libd_transport *trans,
                  size_t size, uint64_t addr, void * buf) {
    get_local_state(rstate,trans,struct tcp_rdma_state);
    return _request(rstate->conn, size, IBV_WR_RDMA_WRITE,
                    (uint64_t)buf, addr);
}

static int _not_implemented_async_write() {
    return -1;
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


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>
#include <infiniband/verbs.h>

#include "libd.h"
#include "libd_transport.h"
#include "interfaces/libd_trdma.h"
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

static int client_exchange_info(struct rdma_conn *conn, const char * url) {
    int sock, rv;
    int send_size, bytes;
    void *info, *peerinfo = NULL;

    // init nanomsg socket
    dprintf("connecting to server %s...", url);
    if ((sock = nn_socket(AF_SP, NN_REQ)) < 0) {
        fatal("nn_socket");
    }

    if ((rv = nn_connect(sock, url)) < 0) {
        fatal("nn_connect");
    }

    send_size = extract_info(conn, &info);
    dprintf("try to send size %d", send_size);
    if ((bytes = nn_send(sock, info, send_size, 0)) < 0) {
        fatal("nn_send");
    }
    free(info);

    if ((bytes = nn_recv(sock, &peerinfo, NN_MSG, 0)) < 0) {
        fatal("nn_recv");
    }

    conn->peerinfo = malloc(bytes);
    memcpy(conn->peerinfo, peerinfo, bytes);
    dprintf("received mr, with bytes %d, num %d", bytes, conn->peerinfo->num_mr);

    // TODO: free msg
    nn_freemsg(peerinfo);

    return nn_shutdown(sock, 0);
}

// TODO: async requests
static inline int _request(struct rdma_conn * conn, size_t size, int opcode,
                uint64_t local_addr, uint64_t remote_addr) {
    int ret;
    struct ibv_wc wc;
    int bytes;

    // SGE for request, we use only 1
    struct ibv_sge sge;
    sge.addr = local_addr;
    sge.length = size;
    sge.lkey = conn->mr[0].lkey;

    struct ibv_send_wr wr, *badwr = NULL;

    wr.wr_id = 233;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    wr.wr.rdma.remote_addr = conn->peerinfo->mr[0].addr + remote_addr;
    wr.wr.rdma.rkey = conn->peerinfo->mr[0].rkey;
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
    init_config_for(trans, struct uverbs_rdma_state);
    get_local_state(rstate,trans,struct uverbs_rdma_state);
    init_config_set(num_devices, 2);
    init_config_set(device_name, "mlx5_1");
    init_config_set(cq_size, 16);

    // init the RDMA connection
    memset(&rstate->conn, 0, sizeof(struct rdma_conn));
    rstate->conn.gid = 0;
    rstate->conn.port = 1;

    // init using the global context and PD
    if (_context == NULL)
        _context = create_context(rstate->num_devices, rstate->device_name);
    rstate->conn.context = _context;
    if (rstate->conn.context == NULL) {
        dprintf("create context fail");
        return -1;
    }

    if (_pd == NULL){
        _pd = ibv_alloc_pd(_context);
        dprintf("allocate pd at %p", _pd);
    }
    rstate->conn.pd = _pd;
    if (rstate->conn.pd == NULL) {
        dprintf("create pd fail");
        return -1;
    }

    // Create QP
    init_config_require(url, id);

    rstate->conn.cq = ibv_create_cq(
        rstate->conn.context, rstate->cq_size, NULL, NULL, 0);

    create_qp(&rstate->conn);
    dprintf("Finish RDMA configuration..");

    return 0;
}

static int _connect(struct libd_transport *trans) {
    get_local_state(rstate,trans,struct uverbs_rdma_state);

    // Exchange with server
    client_exchange_info(&rstate->conn, rstate->url);

    // moving the stats of QP
    qp_stm_reset_to_init (&rstate->conn);
    qp_stm_init_to_rtr   (&rstate->conn);
    qp_stm_rtr_to_rts    (&rstate->conn);

    // Connect to server
    dprintf("Setup connection");
        
    return 0;
}

static int _terminate(struct libd_transport * trans) {
    get_local_state(rstate,trans,struct uverbs_rdma_state);
    // TODO: cleanup local data structure

    // clean up nanomsg
    if (rstate->conn.peerinfo != NULL)
        free(rstate->conn.peerinfo);
    // clean up RDMA
    for (int i = 0; i < rstate->conn.num_mr; i++) {
        ibv_dereg_mr(rstate->conn.mr + i);
    }

    // TODO: close qp and cq, instead of context
    ibv_destroy_qp(rstate->conn.qp);
    ibv_destroy_cq(rstate->conn.cq);

    return 0;
}

// actions
static void * _reg(struct libd_transport *trans, size_t s, void *buf) {
    get_local_state(rstate,trans,struct uverbs_rdma_state);

    if (buf == NULL) {
        // This buffer will be fine if its in the same PD
        create_mr(&rstate->conn, s, IBV_ACCESS_LOCAL_WRITE);
        buf = (void *) (rstate->conn.mr[0].addr);
    }
    return buf;
}

static int _read(struct libd_transport *trans,
                size_t size, uint64_t addr, void * buf) {
    get_local_state(rstate,trans,struct uverbs_rdma_state);
    return _request(&rstate->conn, size, IBV_WR_RDMA_READ,
                    (uint64_t)buf, addr);
}

static int _write(struct libd_transport *trans,
                  size_t size, uint64_t addr, void * buf) {
    get_local_state(rstate,trans,struct uverbs_rdma_state);
    return _request(&rstate->conn, size, IBV_WR_RDMA_WRITE,
                    (uint64_t)buf, addr);
}

static int _not_implemented_async_write() {
    return -1;
}

// export struct
struct libd_trdma rdma_uverbs = {
    .trans = {
        .init = _init,
        .terminate = _terminate,
        .connect = _connect
    },

    .reg = _reg,
    .read = _read,
    .write = _write
};


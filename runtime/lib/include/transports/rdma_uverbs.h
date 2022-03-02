#ifndef _RDMA_UVERBS_H
#define _RDMA_UVERBS_H

#include "libd.h"
#include "libd_transport.h"
#include <infiniband/verbs.h>

#define RDMA_PROTOCOL_IB (0)

// if roce, run on host, use GID 3, else GID 0
#define RDMA_NUM_DEVICES 2
#define RDMA_DEVICE_NAME "mlx5_1"
#define RDMA_GID (3)
#define RDMA_PORT (1)

struct rdma_conn {
    // Those fields should be static
    struct ibv_context* context;
    struct ibv_pd* pd;

    struct ibv_cq* cq;
    struct ibv_qp* qp;

    int num_mr;
    struct ibv_mr* mr;

    int num_buf;
    void ** buf;

    struct conn_info* peerinfo;

    // info for ib/eth
    int gid;
    int port;
};

struct conn_info {
    int port;
    uint32_t local_id;
    uint32_t qp_number;

    int num_mr;
    union ibv_gid gid;
    struct ibv_mr mr[0];
};

struct uverbs_rdma_state {
    struct libd_tstate tstate;

    // RDMA conn info
    struct rdma_conn conn;
    // Conns for 
    int num_conns;
    struct rdma_conn **conns;

    // RDMA configs
    const char * device_name;
    const char * url;
    int num_devices;
    int cq_size;

    size_t size;
    const char * peerinfo;
};

// Functions for rdma_common.c
struct ibv_context *create_context(int num_devices, const char *device_name);

int create_qp(struct rdma_conn *conn);
int create_mr(struct rdma_conn *conn, size_t size, int access, void *buffer);

int qp_stm_reset_to_init(struct rdma_conn *conn);
int qp_stm_init_to_rtr(struct rdma_conn *conn);
int qp_stm_rtr_to_rts(struct rdma_conn *conn);

int extract_info(struct rdma_conn *conn, void **buf);
int extract_info_inplace(struct rdma_conn *conn, void *buf, size_t size);

#endif


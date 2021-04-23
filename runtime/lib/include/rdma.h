#ifndef _SERVERLESS_RDMA_H_
#define _SERVERLESS_RDMA_H_

#include <infiniband/verbs.h>

#define RDMA_PROTOCOL_IB (0)

struct rdma_conn {
    struct ibv_context* context;
    struct ibv_pd* pd;
    struct ibv_cq* cq;
    struct ibv_qp* qp;

    int num_mr;
    struct ibv_mr* mr;

    int port;
    struct conn_info* peerinfo;
};

struct conn_info {
    int port;
    uint32_t local_id;
    uint16_t qp_number;

    int num_mr;
    union ibv_gid gid;
    struct ibv_mr mr[0];
};

#endif

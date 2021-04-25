#include "libd.h"

// interfaces and implementations
extern struct libd_trdma rdma_tcp;
extern struct libd_trdma_server rdma_tcp_server;

extern struct libd_trdma rdma_uverbs;
extern struct libd_trdma rdma_uverbs_server;

// register implementations
const char * libd_transports_name[] = {
    "rdma_tcp",
    "rdma_tcp_server",
    "rdma_uverbs",
    "rdma_uverbs_server",
};
const struct libd_t * libd_transports[] = {
    ((struct libd_t *)&rdma_tcp),
    ((struct libd_t *)&rdma_tcp_server),
    ((struct libd_t *)&rdma_uverbs),
    ((struct libd_t *)&rdma_uverbs_server),
};

const int num_transports = sizeof(libd_transports_name) / sizeof(char *);

#include "libd.h"

extern struct libd_trdma rdma_tcp;
extern struct libd_trdma_server rdma_tcp_server;

const char * libd_transports_name[] = {
    "rdma_tcp",
    "rdma_tcp_server",
};
const struct libd_t * libd_transports[] = {
    ((struct libd_t *)&rdma_tcp),
    ((struct libd_t *)&rdma_tcp_server),
};

const int num_transports = sizeof(libd_transports_name) / sizeof(char *);

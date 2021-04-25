#ifndef _LIBD_TRDMA_SERVER_H_
#define _LIBD_TRDMA_SERVER_H_

#include "libd.h"

// RDMA, only read and write
struct libd_trdma_server {
    struct libd_t trans;
    int (*serve)(struct libd_transport *);
};

// transport APIs, user will call these transport APIs
int libd_trdma_server_serve (struct libd_transport *trans);

#endif


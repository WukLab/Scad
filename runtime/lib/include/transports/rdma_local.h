#ifndef _RDMA_LOCAL_H
#define _RDMA_LOCAL_H

#include "libd.h"
#include "libd_transport.h"

struct local_rdma_state {
    struct libd_tstate tstate;

    // Those fields should be static
    void * mem;
    char * shm_id;
    int size;
};


#endif


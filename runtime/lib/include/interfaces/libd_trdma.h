#ifndef _LIBD_TRDMA_H_
#define _LIBD_TRDMA_H_

#include "libd.h"
#include "stdatomic.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

// RDMA, only read and write
struct libd_trdma {
    struct libd_t trans;
    atomic_uint id_head, id_cnt;
    void * (*reg)(struct libd_transport *, size_t, void *);
    int (*read)(struct libd_transport *, size_t, uint64_t, void *);
    int (*write)(struct libd_transport *, size_t, uint64_t, void *);
    int (*write_async)(struct libd_transport *, size_t, uint64_t, void *, int);
    int (*read_async)(struct libd_transport *, size_t, uint64_t, void *, int);
    int (*poll)(int);
};

// transport APIs, user will call these transport APIs
void * libd_trdma_reg   (struct libd_transport * trans, size_t size, void * buf);
int libd_trdma_read  (struct libd_transport * trans, size_t size, uint64_t addr, void * buf);
int libd_trdma_write (struct libd_transport * trans, size_t size, uint64_t addr, void * buf);
int libd_trdma_poll (int id);
int libd_trdma_write_async (struct libd_transport * trans, size_t size, uint64_t addr, void * buf);
int libd_trdma_read_async (struct libd_transport * trans, size_t size, uint64_t addr, void * buf);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif


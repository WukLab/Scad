#ifndef _LIBD_TRDMA_H_
#define _LIBD_TRDMA_H_

#include "libd.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

// RDMA, only read and write
struct libd_trdma {
    struct libd_t trans;
    void * (*reg)(struct libd_transport *, size_t, void *);
    int (*read)(struct libd_transport *, size_t, uint64_t, void *);
    int (*write)(struct libd_transport *, size_t, uint64_t, void *);
};

// transport APIs, user will call these transport APIs
void * libd_trdma_reg   (struct libd_transport * trans, size_t size, void * buf);
int libd_trdma_read  (struct libd_transport * trans, size_t size, uint64_t addr, void * buf);
int libd_trdma_write (struct libd_transport * trans, size_t size, uint64_t addr, void * buf);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif


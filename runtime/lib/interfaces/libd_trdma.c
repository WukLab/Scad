#include <errno.h>

#include "libd.h"
#include "libd_transport.h"
#include "interfaces/libd_trdma.h"
#include "stdatomic.h"

#define operation_start(ret,trans,cur) \
    operation(ret,trans,cur); \
    if (!ret) return -EAGAIN

// TODO: blocking interface?
// interface functions
int libd_trdma_write (struct libd_transport * trans, size_t size, uint64_t addr, void * buf) {
    int ret;

    operation_start(ret, trans, LIBD_TRANS_STATE_READY);
    
    ret = transport_handler(libd_trdma, trans, write)(trans, size, addr, buf);
    if (ret != 0) {
        abort();
        return ret;
    }

    
    trans->tstate->counters.tx_bytes += size;
    success();
    return ret;
}

int libd_trdma_read  (struct libd_transport * trans, size_t size, uint64_t addr, void * buf) {
    int ret;

    operation_start(ret, trans, LIBD_TRANS_STATE_READY);

    ret = transport_handler(libd_trdma, trans, read)(trans, size, addr, buf);
    if (ret != 0) {
        abort();
        return ret;
    }

    trans->tstate->counters.rx_bytes += size;
    success();
    return ret;
}

int libd_trdma_read_async (struct libd_transport * trans, size_t size, uint64_t addr, void * buf) {
#ifdef ENABLE_ASYNC
    int ret;

    operation_start(ret, trans, LIBD_TRANS_STATE_READY);
    
    unsigned int id = atomic_fetch_add(&trans->id_head, 1);
    ret = transport_handler(libd_trdma, trans, read_async)(trans, size, addr, buf, id);
    if (ret != 0) {
        abort();
        return ret;
    }

    trans->tstate->counters.rx_bytes += size;
    success();
    return ret;
#else
    return -EINVAL;
#endif /* ENABLE_ASYNC */
}

int libd_trdma_write_async (struct libd_transport * trans, size_t size, uint64_t addr, void * buf) {
#ifdef ENABLE_ASYNC
    int ret;

    operation_start(ret, trans, LIBD_TRANS_STATE_READY);
    
    unsigned int id = atomic_fetch_add(&trans->id_head, 1);
    ret = transport_handler(libd_trdma, trans, read_async)(trans, size, addr, buf, id);
    if (ret != 0) {
        abort();
        return ret;
    }
    
    trans->tstate->counters.tx_bytes += size;
    success();
    return ret;
#else
    return -EINVAL;
#endif /* ENABLE_ASYNC */
}

int libd_trdma_poll (int id) {
#ifdef ENABLE_ASYNC
    int ret;

    operation_start(ret, trans, LIBD_TRANS_STATE_READY);
    
    ret = transport_handler(libd_trdma, trans, poll)(id);
    if (ret != 0) {
        abort();
        return ret;
    }
    
    success();
    return ret;
#else
    return -EINVAL;
#endif /* ENABLE_ASYNC */
}

void * libd_trdma_reg   (struct libd_transport * trans, size_t size, void * buf) {
    // TODO: make this also check status
    return transport_handler(libd_trdma, trans, reg)(trans, size, buf);
}

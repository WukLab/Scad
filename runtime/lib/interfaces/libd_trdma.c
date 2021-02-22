#include <errno.h>

#include "libd.h"
#include "libd_trdma.h"
#include "libd_transport.h"

// TODO: blocking interface?
// interface functions
int libd_trdma_write (struct libd_transport * trans, size_t size, uint64_t addr, void * buf) {
    int ret;

    // TODO: blocking?
    operation_spin(ret, trans, LIBD_TRANS_STATE_READY);

    // metrics
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

    // TODO: blocking?
    operation_spin(ret, trans, LIBD_TRANS_STATE_READY);

    ret = transport_handler(libd_trdma, trans, read)(trans, size, addr, buf);
    if (ret != 0) {
        abort();
        return ret;
    }

    trans->tstate->counters.rx_bytes += size;
    success();
    return ret;
}

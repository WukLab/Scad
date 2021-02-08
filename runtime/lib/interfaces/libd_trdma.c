#include <errno.h>

#include "libd.h"
#include "libd_trdma.h"
#include "libd_transport.h"

// interface functions
int libd_trdma_write (struct libd_transport * trans, size_t size, uint64_t addr, void * buf) {

    if (trans->tstate->state != LIBD_TRANS_STATE_READY)
        return -EINVAL;

    return transport_handler(libd_trdma, trans, write)(trans, size, addr, buf);
}

int libd_trdma_read  (struct libd_transport * trans, size_t size, uint64_t addr, void * buf) {

    if (trans->tstate->state != LIBD_TRANS_STATE_READY)
        return -EINVAL;

    return transport_handler(libd_trdma, trans, read)(trans, size, addr, buf);
}

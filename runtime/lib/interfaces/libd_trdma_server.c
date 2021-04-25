#include <errno.h>

#include "libd.h"
#include "libd_transport.h"
#include "libd_trdma_server.h"

// interface functions
int libd_trdma_server_serve (struct libd_transport *trans) {
    if (trans->tstate->state != LIBD_TRANS_STATE_READY) {
        dprintf("Error in state, found %d", trans->tstate->state);
        return -EINVAL;
    }

    return transport_handler(libd_trdma_server, trans, serve)(trans);
}

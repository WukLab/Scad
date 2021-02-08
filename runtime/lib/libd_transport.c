#include <stdio.h>
#include <errno.h>

#include "libd.h"
#include "libd_transport.h"

// Util functions
char * tstate_config_get (struct libd_tstate * state, char * key) {
    return map_get(char, state->config, key);
}

// state modification functions
int libd_transport_modify(struct libd_transport * trans, int from, int to) {
    if (from != LIBD_TRANS_STATE_ANY && trans->tstate->state != from)
        return -EINVAL;
    if (to != LIBD_TRANS_STATE_ANY)
        trans->tstate->state = to;
    return trans->tstate->state;
}

// State machine functions
// Handles lock and state mchine; state can also be changed from internal
// TODO: add locks here
int libd_transport_init      (struct libd_transport * trans) {
    int ret;

    if (trans->tstate->state != LIBD_TRANS_STATE_INIT)
        return -EINVAL;

    if ((ret = trans->_impl->init(trans)) < 0)
        return ret;

    trans->tstate->state = LIBD_TRANS_STATE_INITD;
    return 0;
}

int libd_transport_connect   (struct libd_transport * trans) {
    int ret;

    if (trans->tstate->state != LIBD_TRANS_STATE_INITD)
        return -EINVAL;

    if ((ret = trans->_impl->connect(trans)) < 0)
        return ret;

    trans->tstate->state = LIBD_TRANS_STATE_READY;
    return 0;
}

int libd_transport_recover   (struct libd_transport * trans) {
    return -ENOSYS;
}

int libd_transport_terminate (struct libd_transport * trans) {
    if ((trans->_impl->terminate(trans)) < 0)
        return 0;

    trans->tstate->state = LIBD_TRANS_STATE_TERMINATED;
    return 0;
}

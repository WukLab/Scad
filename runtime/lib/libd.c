#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "libd.h"
#include "map.h"
#include "durl.h"

struct libd_action * libd_action_init(char * aid, char * server_url) {
    struct libd_action * action =
        (struct libd_action *)calloc(1, sizeof(struct libd_action));
    map_init(action->transports, string);

    strcpy(action->aid, aid);
    strcpy(action->server_url, server_url);
    return action;
}

int libd_action_free(struct libd_action * action) {
    int ret;
    for (int i = 0; i < action->transports.size; i++) {
        struct libd_transport * trans = action->transports.values[i];
        if ((ret = libd_transport_terminate(trans)) != 0)
            return ret;
    }

    map_free(action->transports);
    free(action);
    return 0;
}

int libd_action_config_transport(struct libd_action *action, char *name, char *durl) {
    int ret;
    struct libd_transport *trans = libd_action_get_transport(action, name);
    if ((ret = parse_config(trans->tstate, durl)) < 0)
        return ret;

    // 3. try to move to running state, its ok if it fails
    ret = LIBD_TRANS_STATE_INIT;
    if ((ret = libd_transport_init(trans)) < 0)
        return trans->tstate->state;
    if ((ret = libd_transport_connect(trans)) < 0)
        return trans->tstate->state;

    return trans->tstate->state;
}

struct libd_transport * libd_action_get_transport(struct libd_action * action, char * name) {
    return map_get(struct libd_transport, action->transports, name);
}

// find impl and add action
int libd_action_add_transport(struct libd_action * action, char * durl) {

    int ret;

    struct libd_transport *trans;
    trans = (struct libd_transport *)calloc(sizeof(struct libd_transport), 1);

    // 1. parse the given parameters
    trans->tstate = parse_durl(durl);
    trans->tstate->state = LIBD_TRANS_STATE_INIT;

    if (trans->tstate == NULL)
        return -EINVAL;

    // 2. find implementation of the given type
    for (int i = 0; i < num_transports; i++) {
        dprintf("name %s cur %s\n",
            trans->tstate->impl, libd_transports_name[i]);
        if (strcmp(libd_transports_name[i], trans->tstate->impl) == 0) {
            trans->_impl = libd_transports[i];
        }
    }
    if (trans->_impl == NULL) {
        ret = -ENOENT;
        goto cleanup;
    }

    map_insert(action->transports, trans->tstate->name, trans);
    dprintf("Init Transport with name %s", trans->tstate->name);

    // 3. try to move to running state, its ok if it fails
    ret = LIBD_TRANS_STATE_INIT;
    if ((ret = libd_transport_init(trans)) < 0) {
        dprintf("init fail with rv %d", ret);
        return trans->tstate->state;
    }
    if ((ret = libd_transport_connect(trans)) < 0) {
        dprintf("connect fail with rv %d", ret);
        return trans->tstate->state;
    }

    return trans->tstate->state;

cleanup:
    // TODO: free the memories
    return ret;
}


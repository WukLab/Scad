#include <string.h>
#include <stdlib.h>
#include <errno.h>

#include "libd.h"
#include "map.h"
#include "durl.h"

static void _init_plugin (struct libd_action *action, const char * name) {
    struct libd_plugin * plugin = calloc(1, sizeof(struct libd_plugin));
    plugin->pstate = NULL;
    plugin->action = action;

    for (int j = 0; j < num_plugins; j++) {
        if (strcmp(libd_plugins_name[j], name) == 0) {
            dprintf("plugin init %s", name);
            map_insert(action->plugins, name, plugin);
            plugin->_impl = libd_plugins[j];
            plugin->_impl->init(plugin);
            return;
        }
    }

    // If we reach this point, we cannot find valid impl
    free(plugin);
}

struct libd_action * libd_action_init(char * aid, int argc, char ** args) {
    struct libd_action * action =
        (struct libd_action *)calloc(1, sizeof(struct libd_action));

    strcpy(action->aid, aid);
    map_init(action->transports, string);

    // TODO: init of plugins
    map_init(action->plugins, string);

    for (int i = 0; i < argc; i++) {
        if (args[i][0] == '+') {
            dprintf("setting option %s for action %s", args[i], aid);
            // active plugins
            if (strcmp("+plugins", args[i]) == 0) {
                for (++i; i < argc && args[i][0] != '+'; i++)
                    _init_plugin(action, args[i]);
            }
            // set post url
            else if (strcmp("+post_url", args[i]) == 0) {
                strcpy(action->post_url, args[++i]);
            }
            else if (strcmp("+server_url", args[i]) == 0) {
                strcpy(action->server_url, args[++i]);
            }
        }
    }

    dprintf("Action %s initd with argc %d\n", aid, argc);
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

    // TODO: free of plugins
    for (int i = 0; i < action->plugins.size; i++) {
        struct libd_plugin * plugin = action->plugins.values[i];
        plugin->_impl->terminate(plugin);
        free(plugin);
    }
    map_free(action->plugins);

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
    struct libd_transport * trans; 
    // TODO: block here will cause timeout in node runtime
    while ((trans = map_get(struct libd_transport, action->transports, name)) == NULL) ;
    return trans;
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


#include <stdlib.h>
#include <errno.h>
#include "libd.h"

struct libd_plugin * libd_plugin_init(struct libd_p *impl) {
    int ret;
    struct libd_plugin *plugin = calloc(1, sizeof(struct libd_plugin));
    struct libd_pstate *pstate = calloc(1, sizeof(struct libd_pstate));

    plugin->_impl = impl;
    plugin->pstate = pstate;

    if ((ret = impl->init(plugin)) != 0) {
        if (plugin->pstate != NULL)
            free(plugin->pstate);
        free(plugin);
        plugin = NULL;
    }
    return plugin;
}

int libd_plugin_ternimate(struct libd_plugin *plugin) {
    int ret;
    ret = plugin->_impl->terminate(plugin);
    if (ret < 0)
        return ret;

    if (plugin->pstate != NULL)
        free(plugin->pstate);
    free(plugin);
    return 0;
}

int libd_plugin_invoke(struct libd_action * action,
        const char * name, int cmd, void * args) {
    struct libd_plugin * plugin;

    if ((plugin = map_get(struct libd_plugin, action->plugins, name))
            != NULL) {
        return plugin->_impl->invoke(plugin, cmd, args);
    }

    dprintf("invoke: error: cannot find plugin %s", name);
    return -EINVAL;
}


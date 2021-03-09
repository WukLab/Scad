#include "libd.h"

struct libd_plugin * libd_plugin_init(struct libd_p *impl) {
    int ret;
    struct libd_plugin *plugin = calloc(sizeof(struct libd_plugin));
    struct libd_pstate *pstate = calloc(sizeof(struct libd_pstate));

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
}

plugin_cbfunc libd_plugin_callback_match(const char *pattern, const char * trans_name) {

    const char * state = strchr(pattern, ':');
    if (state == NULL) {
        return NULL;
    }
    
}


#include "libd.h"

// interfaces and implementations
extern struct libd_p monitor_plugin;

// register implementations
const char * libd_plugins_name[] = {
    "monitor",
};

const struct libd_p * libd_plugins[] = {
    monitor_plugin,
};

const int num_plugins = sizeof(libd_transports_name) / sizeof(char *);

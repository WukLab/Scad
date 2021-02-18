#ifndef _LIBD_H_
#define _LIBD_H_

#include <pthread.h>
#include <stdint.h>
#include <stdatomic.h>
#include "map.h"

#ifdef DEBUG
    #include <stdio.h>
    #define dprintf(t, args...) \
        fprintf(stderr, "[%s:%d] " t "\n", __FILE__, __LINE__, ## args)
#else
    #define dprintf(...)
#endif /* DEBUG */

// Transport, Message, Stream, RDMA, static represents
extern const char * libd_transports_name[];
extern const struct libd_t *libd_transports[];
extern const int num_transports;

struct libd_counters {
    uint64_t tx_bytes;
    uint64_t rx_bytes;
};

enum {
    LIBD_TRANS_STATE_ACTION = -3,
    LIBD_TRANS_STATE_TRANSIT = -2,
    LIBD_TRANS_STATE_ANY = -1,
    LIBD_TRANS_STATE_INIT = 0,
    LIBD_TRANS_STATE_INITD,
    LIBD_TRANS_STATE_CONNECT,
    LIBD_TRANS_STATE_READY,
    LIBD_TRANS_STATE_ERROR,
    LIBD_TRANS_STATE_TERMINATED
};

struct libd_tstate {
    struct libd_counters counters;
    atomic_int state;
    
    char *name, *impl;
    map_of(string,string) config;
};

struct libd_transport {
    // TODO: multi threading
    pthread_mutex_t lock;           // lock for tstate
    pthread_t pt;

    struct libd_tstate * tstate;
    const struct libd_t * _impl;
};

struct libd_t {
    int (*init)      (struct libd_transport *);
    int (*connect)   (struct libd_transport *);
    int (*terminate) (struct libd_transport *);
};

// void * libd_transport_modify (struct libd_transport, int target, void ** args);
// State machine

int libd_transport_init      (struct libd_transport * trans); 
int libd_transport_connect   (struct libd_transport * trans);
int libd_transport_recover   (struct libd_transport * trans); // error -> init
int libd_transport_terminate (struct libd_transport * trans); // any -> terminate

#define transport_handler(IMPL,t,HNDL) (((struct IMPL *)((t)->_impl))->HNDL)

// action
struct libd_action {
    char aid[128];
    char server_url[256];
    map_of(string, struct libd_transport *) transports;
    map_of(string, int (*)(struct libd_plugin *, struct libd_transport *)) transport_hooks;
};

struct libd_action * libd_action_init(char * aid, char * server_url);
int libd_action_free(struct libd_action * action);

int libd_action_add_transport(struct libd_action * action, char * durl);
int libd_action_config_transport(struct libd_action * action, char *name, char * durl);
struct libd_transport * libd_action_get_transport(struct libd_action * action, char * name);

// user functions
struct libd_plugin {
    struct libd_action *action;
    int (*init) (struct libd_plugin *);
    int (*terminate) (struct libd_plugin *);
};

// register a callback for transport
int libd_plugin_reg_transport(struct libd_plugin *plugin, char *name,
                              int (*callback)(struct libd_plugin *, struct libd_transport *);

#endif

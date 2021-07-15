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

/* Plugin */
// user functions
extern const char * libd_plugins_name[];
extern const struct libd_p * libd_plugins[];
extern const int num_plugins;

// TODO: helper function for pstate? this init is non-trival
struct libd_plugin {
    struct libd_pstate *pstate;
    struct libd_action *action;

    const struct libd_p * _impl;
};

struct libd_p {
    int (*init)      (struct libd_plugin *);
    int (*terminate) (struct libd_plugin *);
    int (*invoke)    (struct libd_plugin *, int, void *);
};

// when adding transport, need to put this one at right place!
struct libd_pstate {
    int _ph;
};

struct libd_transport;
typedef int (*plugin_cbfunc) (struct libd_plugin *, struct libd_transport *);

struct libd_plugin_callback {
    plugin_cbfunc func;
    struct libd_plugin * plugin;

    int state;
};

// Call plugin from outside
int libd_plugin_invoke(struct libd_action * action,
        const char * name, int cmd, void * args);
// register a callback for transport
int libd_plugin_reg_callback(struct libd_plugin *plugin, struct libd_transport *trans,
                             struct libd_plugin_callback *callback);

/* Transport */
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
    map_of(string,string) config;

    struct libd_counters counters;
    struct libd_plugin_callback callback;

    char *name, *impl;
    atomic_int state;
};

struct libd_transport {
    struct libd_tstate * tstate;
    const struct libd_t * _impl;

    // TODO: multi threading
    pthread_mutex_t lock;           // lock for tstate
    pthread_t pt;

    char initd;
};

struct libd_t {
    int (*init)      (struct libd_transport *);
    int (*connect)   (struct libd_transport *);
    int (*terminate) (struct libd_transport *);
};

// void * libd_transport_modify (struct libd_transport, int target, void ** args);
// State machine

int libd_transport_query     (struct libd_transport * trans); 

int libd_transport_init      (struct libd_transport * trans); 
int libd_transport_connect   (struct libd_transport * trans);
int libd_transport_recover   (struct libd_transport * trans); // error -> init
int libd_transport_terminate (struct libd_transport * trans); // any -> terminate

#define transport_handler(IMPL,t,HNDL) (((struct IMPL *)((t)->_impl))->HNDL)

/* Action */
// action
struct libd_action {
    char aid[128];
    char server_url[256];
    char post_url[256];

    map_of(string, struct libd_transport *) transports;
    map_of(string, struct libd_plugin *) plugins;
};

struct libd_action * libd_action_init(char * aid, int argc, char ** argv);
int libd_action_free(struct libd_action * action);

int libd_action_add_transport(struct libd_action * action, char * durl);
int libd_action_config_transport(struct libd_action * action, char *name, char * durl);
struct libd_transport * libd_action_get_transport(struct libd_action * action, char * name);

#endif

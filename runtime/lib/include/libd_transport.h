#ifndef _LIBD_TRANSPORT_H_
#define _LIBD_TRANSPORT_H_

#ifdef DEBUF
	#inlcude "libd.h"
#endif

// utils
char * tstate_config_get (struct libd_tstate * state, char * config);
int libd_transport_modify(struct libd_transport * trans, int from, int to);

// Macros for defining the config
#define init_config_for(t,tt) \
    (t)->tstate = (struct libd_tstate *)realloc((t)->tstate,sizeof(tt)); \
    struct libd_tstate * _state = (t)->tstate; \
    tt * _config = (tt *)((t)->tstate); \
    char * _value

#define init_config_require(name,f) \
    _value = tstate_config_get(_state, #name); \
    if (_value == NULL) { \
        dprintf("init parameter missing %s", #name); \
        return -1; \
    } \
    _config->name = f(_value);

#define init_config_set(name,v) \
    _config->name = v;

#define id(a) (a)
#define libd_reg_transport(name, impl)

#define get_local_state(name,t,tt) tt *name = (tt *)((t)->tstate)

#endif

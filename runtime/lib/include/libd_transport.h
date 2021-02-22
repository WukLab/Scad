#ifndef _LIBD_TRANSPORT_H_
#define _LIBD_TRANSPORT_H_

#include <stdatomic.h>

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

// Functions for atomic transition
// transit and operation (acquire) is non-blocking
// success and abort (release) is blocking
#define transit(ret, trans, from, to) \
    int __as_fail = from, __as_succ = to, __as_lock = LIBD_TRANS_STATE_TRANSIT; \
    ret = atomic_compare_exchange_weak(&(trans->tstate->state), &__as_fail, __as_lock)
#define operation(ret, trans, cur) \
    int __as_fail = LIBD_TRANS_STATE_ERROR, __as_succ = cur, __as_lock = LIBD_TRANS_STATE_ACTION; \
    ret = atomic_compare_exchange_weak(&(trans->tstate->state), &__as_succ, __as_lock)
#define operation_spin(ret, trans, cur) \
    int __as_fail = LIBD_TRANS_STATE_ERROR, __as_succ = cur, __as_lock = LIBD_TRANS_STATE_ACTION; \
    while (!(ret = atomic_compare_exchange_weak(&(trans->tstate->state), &__as_succ, __as_lock))) ;
#define success() \
    while (!atomic_compare_exchange_weak(&(trans->tstate->state), &__as_lock, __as_succ)) ;
#define abort() \
    while (!atomic_compare_exchange_weak(&(trans->tstate->state), &__as_lock, __as_fail)) ;

#endif

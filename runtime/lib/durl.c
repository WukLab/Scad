#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include "libd.h"
#include "map.h"

static int parse_string(char **curp, char **dstp, char sep) {
    char *pos, *cur = *curp, *dst;
    int size;

    if ((pos = strchr(cur, sep)) == NULL)
        return -1;
    size = pos - cur + 1;

    dst = (char *)malloc(size);
    strncpy(dst, cur, size - 1);
    dst[size - 1] = '\0';

    *curp = pos + 1;
    *dstp = dst;
    return size;
}

// DURL: NAME;IMPL;KEY,VALUE;KEY,VALIE;
int parse_config(struct libd_tstate *state, char *cur) {
    char *key, *value;
    while (parse_string(&cur, &key, ',') != -1) {
        if (parse_string(&cur, &value, ';') < 0)
            return -EINVAL;
        map_insert(state->config, key, value);
        dprintf("parsing config %s %s", key, value);
    }

    if (*cur != '\0')
        return -EINVAL;


    return 0;
}

struct libd_tstate * parse_durl(char * durl) {
    char *cur = durl;

    dprintf("parsing durl %s", durl);
    struct libd_tstate *state =
        (struct libd_tstate *)malloc(sizeof(struct libd_tstate));
    map_init(state->config, string);

    parse_string(&cur, &state->name, ';');
    parse_string(&cur, &state->impl, ';');

    if (parse_config(state, cur) != 0)
        return NULL;

    return state;
}


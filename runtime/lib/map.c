#include <string.h>
#include <stdlib.h>

#include "map.h"

static const int MAP_DEFAULT_SIZE = 4;

int _map_init(map *m, int ksize, const char * kt) {
    m->cap = MAP_DEFAULT_SIZE;
    m->keys   = (void **)calloc(MAP_DEFAULT_SIZE, sizeof(void *));
    m->values = (void **)calloc(MAP_DEFAULT_SIZE, sizeof(void *));
    
    if (strcmp(kt, "string") == 0)
        m->ksize = -1;
    else
        m->ksize = ksize;

    return 0;
}

int _map_insert(map *m, void *key, void *value) {
    if (_map_get(m, key) != NULL)
        return -1; 
    
    if (m->size == m->cap) {
        m->cap *= 2;
        m->keys = (void **)realloc(m->keys, sizeof(void *) * m->cap);
        m->values = (void **)realloc(m->values, sizeof(void *) * m->cap);
    }

    m->keys[m->size] = key;
    m->values[m->size] = value;
    return m->size ++;
}

void * _map_get(map *m, void *key) {
    int ksize = m->ksize;
    for (int i = 0; i < m->size; i++) {
        int ret = 0;
        void * ki = m->keys[i];

        if (ksize < 0)
            ret = strcmp(ki, key);
        else
            ret = memcmp(ki, key, ksize);
            
        if (ret == 0)
            return m->values[i];
    }
    return NULL;
}

int _map_free(map *m) {
    free(m->keys);
    free(m->values);
    return 0;
}

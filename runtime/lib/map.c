#include <string.h>
#include <stdlib.h>

#include "map.h"

static const int MAP_DEFAULT_SIZE = 4;

// we relies on ksize to see if its string (null terminated)

int _map_init(map *m, int ksize, const char * kt) {
    m->cap = MAP_DEFAULT_SIZE;
    m->size = 0;
    m->keys   = (void **)calloc(MAP_DEFAULT_SIZE, sizeof(void *));
    m->values = (void **)calloc(MAP_DEFAULT_SIZE, sizeof(void *));
    
    if (strcmp(kt, "string") == 0)
        m->ksize = -1;
    else
        m->ksize = ksize;

    return 0;
}

int _map_insert(map *m, const void *key, const void *value) {
    if (_map_get(m, key) != NULL)
        return -1; 
    
    if (m->size == m->cap) {
        m->cap *= 2;
        m->keys = (void **)realloc(m->keys, sizeof(void *) * m->cap);
        m->values = (void **)realloc(m->values, sizeof(void *) * m->cap);
    }

    // copy keys
    int ksize = m->ksize == -1 ? strlen(key) + 1 : m->ksize;
    void * _key = malloc(ksize);
    memcpy(_key, key, ksize);

    m->keys[m->size] = _key;
    m->values[m->size] = value;
    return m->size ++;
}

void * _map_get(map *m, const void *key) {
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
    for (int i = 0; i < m->size; i++)
        free(m->keys[i]);
    free(m->keys);
    free(m->values);
    return 0;
}

#ifdef DEBUG
#include <stdio.h>
void _debug_map_print(map *m) {
    for (int i = 0; i < m->size; i++) {
        printf("%s -> %s\n", (char *)m->keys[i], (char *)m->values[i]);
    }
}
#endif

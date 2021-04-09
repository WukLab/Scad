#ifndef _MAP_H_
#define _MAP_H_

// make the compiler happy
typedef void string;

typedef struct {
    int cap, size, ksize;
    void **keys, **values;
} map;
#define map_of(kt,vt) map

#define map_init(m,kt) (_map_init(&(m),sizeof(kt),#kt))
int _map_init(map *m, int ksize, const char * kt);

#define map_insert(m,k,v) (_map_insert(&(m),(k),(v)))
int _map_insert(map *m, const void *key, const void *value);

#define map_get(vt,m,k) ((vt *)_map_get(&(m),k))
void * _map_get(map *m, const void *key);

/* Do not free the space for keys and values */
#define map_free(m) _map_free(&(m))
int _map_free(map *m);

// TODO: map cleanup
// map_cleanup(m,kd,vd) 

#ifdef DEBUG
#define debug_map_print(m) _debug_map_print(&(m))
void _debug_map_print(map *m);
#else
#define debug_map_print(m)
#endif

#endif

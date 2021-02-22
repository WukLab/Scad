#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <unistd.h>

#include "libd.h"

#define NUM_POINTS      (128)
#define BUFFER_SIZE     (65536)
#define BUFFER_THRESHOLD (1024)

#define add_field(b,k,v) \
    sprintf((b),"%s=%s&",k,v); \
    (b) += strlen(b);

struct monitor_pstate {
    struct libd_pstate pstate;
    char buf[BUFFER_SIZE];

    pthread_t pid;
    int interval;
    char * server_url;
    char * object_name;
    char * curbuf;
};

static void inline init_buf(struct monitor_pstate * monitorp) {
    int bytes;
    monitorp->curbuf= monitorp->buf;
    bytes = sprintf(monitorp->curbuf, "object=%s", monitorp->object_name);
    monitorp->curbuf += bytes;
}

void post_stat(struct monitor_pstate * monitorp) {
    CURL *curl;
    CURLcode res;

    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, monitorp->server_url);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, monitorp->buf);

        res = curl_easy_perform(curl);
        if(res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n",
                    curl_easy_strerror(res));

        curl_easy_cleanup(curl);
    }
}

void * monitor_loop(void * _plugin) {
    struct libd_plugin *plugin =
        (struct libd_plugin *)_plugin; 
    struct monitor_pstate *monitorp =
        (struct monitor_pstate *)(plugin->pstate);

    for (;;) {
        int bytes;
        usleep(monitorp->interval);

        // TODO: map API
        if (plugin->action->transports.size != 0) {
            bytes = sprintf(monitorp->curbuf, "&%lu=", (unsigned long)time(NULL));
            monitorp->curbuf += bytes;

            for (int i = 0; i < plugin->action->transports.size; i++) {
                struct libd_transport *trans = plugin->action->transports.values[i];
                bytes = sprintf(monitorp->curbuf,",%s,%lu,%lu", trans->tstate->name,
                    trans->tstate->counters.tx_bytes, trans->tstate->counters.rx_bytes);
                monitorp->curbuf += bytes;
            }
        }

        if (monitorp->curbuf - monitorp->buf <= BUFFER_THRESHOLD) {
            post_stat(monitorp);
            init_buf(monitorp);
        }
    }

    return NULL;
}


static int _init(struct libd_plugin *plugin) {
    int ret;
    // alloc here
    struct monitor_pstate *monitorp = 
        (struct monitor_pstate *)realloc(plugin->pstate, sizeof(struct monitor_pstate));
    plugin->pstate = (struct libd_pstate *)monitorp;

    ret = pthread_create(&monitorp->pid, NULL, monitor_loop, (void *)plugin);
    if (ret){
        printf("ERROR; return code from pthread_create() is %d\n", ret);
        return ret;
    }

    curl_global_init(CURL_GLOBAL_ALL);
    return 0;
}

static int _terminate(struct libd_plugin *plugin) {
    struct monitor_pstate *monitorp = 
        (struct monitor_pstate *)(plugin->pstate);

    // dealloc here
    plugin->pstate = (struct libd_pstate *)realloc(plugin->pstate, sizeof(struct libd_pstate));

    pthread_cancel(monitorp->pid);
    post_stat(monitorp);

    curl_global_cleanup();
    return 0;
}

struct libd_p monitor_plugin = {
    .init = _init,
    .terminate = _terminate
};


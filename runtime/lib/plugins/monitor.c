#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <curl.h>
#include <unistd.h>

#include "libd.h"

#define NUM_POINTS 128
#define BUFFER_SIZE 65536

struct counter_plugin {
    struct libd_plugin plugin;
    pthread_t pid;
    int interval;
    char * server_url;
    char * object_name;

    // stashed counters
    int cur_counter;
    struct libd_counter counters[NUM_POINTS];
};

#define add_field(b,k,v) \
    sprintf(b,"%s=%s&",k,v); \
    (b) += strlen(b);

int counter_loop(void * plugin) {
    struct counter_plugin *counterp = (struct counter_plugin *)plugin; 
    for (;;) {
        usleep(counterp->interval);

        for (struct libd_transport *trans = counterp->plugin->action->trans; trans != NULL; trans++) {
            memcpy(counterp->counters + counterp->cur_coutner, trans->state->conuter, sizeof(struct libd_counter));
            ++ (counterp->cur_counter);
        }

        if (counterp->cur_counter == NUM_POINTS) {
            counterp->cur_counter = 0;
            post_counter(counterp);
        }
    }
}

void post_counter(struct counter_plugin * counterp) {
    CURL *curl;
    CURLcode res;

    buf = buffer[BUFFER_SIZE];

    // construct buffer
    add_field(cur,"object", counterp->object);
    add_field(cur,"timestamp", itoa(gettime()));
    for (int i = 0; i < counterp->cur_counter; i++) {
        add_field(cur, "counter", counterp->counters[i]);
    }

    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, server_url);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, counters);

        res = curl_easy_perform(curl);
        if(res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n",
                    curl_easy_strerror(res));

        curl_easy_cleanup(curl);
    }
}

int _init(struct libd_plugin *plugin) {
    int ret;
    struct counter_plugin *counterp = (struct counter_plugin *)plugin; 

    ret = pthread_create(&counterp->pid, NULL, fetch_counter, (void *)plugin);
    if (ret){
        printf("ERROR; return code from pthread_create() is %d\n", rc);
        return ret;
    }

    curl_global_init(CURL_GLOBAL_ALL);
    return 0;
}

int _terminate(struct libd_plugin *plugin) {
    pthread_cancel(counter_thread);

    post_counter(plugin);

    curl_global_cleanup();
    return 0;
}

struct libd_plugin monitor = {
    .init = _init,
    .terminate = _terminate
};


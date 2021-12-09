#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>  
#include <unistd.h>
#include <string.h>

#include "libd.h"
#include "libd_transport.h"

#define BUFFER_SIZE      (1024 * 64)
#define BUFFER_THRESHOLD (1024 * 2)
#define IOBUF_SIZE (1024)

#define HERTZ_VALUE (100)
#define ENABLE_MONITORTHREAD 0

static char iobuf[IOBUF_SIZE];

// TODO: change this serverurl
struct monitor_pstate {
    struct libd_pstate pstate;
    char buf[BUFFER_SIZE];

    // old counters for system
    unsigned long cpu_total;
    
    int procf_mem;
    int procf_cpu;
    unsigned long invoke_ts;

    pthread_t pid;
    int interval;
    char * curbuf;
};

static void inline init_buf(
    struct monitor_pstate * monitorp, const char * aid, const char * name) {

    int bytes;
    monitorp->curbuf= monitorp->buf;
    bytes = sprintf(monitorp->curbuf, "object=%s&name=%s", aid, name);
    monitorp->curbuf += bytes;
}

void post_stat(struct monitor_pstate * monitorp, const char * url) {
    CURL *curl;
    CURLcode res;

    *monitorp->curbuf = '\0';
    dprintf("posting stat to %s with size %ld", url, strlen(monitorp->buf));

    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, monitorp->buf);

        res = curl_easy_perform(curl);
        if(res != CURLE_OK)
            fprintf(stderr, "curl_easy_perform() failed: %s\n",
                    curl_easy_strerror(res));

        curl_easy_cleanup(curl);
    }
}

static void inline query_network_usage(char ** bufp, struct libd_action * action) {
    int bytes;
    for (int i = 0; i < action->transports.size; i++) {
        const struct libd_transport *trans =
            action->transports.values[i];
        bytes = sprintf(*bufp, ",%s,%lu,%lu",
            trans->tstate->name,
            trans->tstate->counters.tx_bytes,
            trans->tstate->counters.rx_bytes);
        (*bufp) += bytes;
    }
}

// Code for loop
static void inline query_cpu_usage(char ** bufp, int procf, struct monitor_pstate *monitorp) {
    int bytes;
    unsigned long utime, stime, total, diff;
    long cutime, cstime;

    float ts, tsdiff;
    struct timespec t;

    char *proc_bufp = iobuf;
    lseek (procf, 0, SEEK_SET);
    read  (procf, iobuf, IOBUF_SIZE);
    for (int i = 0; i < 13; i++) {
        proc_bufp = strchr(proc_bufp, ' ');
        proc_bufp++;
    }
    sscanf(proc_bufp, "%lu %lu %ld %ld", &utime, &stime, &cutime, &cstime);
    total = utime + stime + cutime + cstime;

    // get tick difference (by default, 
    diff = total - monitorp->cpu_total;
    monitorp->cpu_total = total;

    // get time diff in milliseconds
    clock_gettime(CLOCK_BOOTTIME, &t);
    ts = t.tv_nsec / 1000000 + t.tv_sec * 1000;

    tsdiff = ts - monitorp->invoke_ts;
    monitorp->invoke_ts = ts;

    float percent = 0;
    if (tsdiff != 0)
        percent = diff * (1000 / HERTZ_VALUE) / tsdiff;

    // print to output
    bytes = sprintf(*bufp, ",%f", percent);
    (*bufp) += bytes;
}

static void inline query_memory_usage(char ** bufp, int procf) {
    int bytes;
    unsigned long vmsize, resident, share, text, lib, data, dirty; 
    lseek(procf, 0, SEEK_SET);
    read(procf, iobuf, IOBUF_SIZE);
    sscanf(iobuf, "%lu %lu %lu %lu %lu %lu %lu",
        &vmsize, &resident, &share, &text, &lib, &data, &dirty);

    uint64_t mem_usage = vmsize;

    bytes = sprintf(*bufp, ",%lu", mem_usage);
    (*bufp) += bytes;
}

void * monitor_loop(void * _plugin) {
    struct libd_plugin *plugin =
        (struct libd_plugin *)_plugin; 
    struct monitor_pstate *monitorp =
        (struct monitor_pstate *)(plugin->pstate);

    for (;;) {
        int bytes;
        usleep(monitorp->interval);

        if (plugin->action->transports.size != 0) {
            bytes = sprintf(monitorp->curbuf, "&%lu=", (unsigned long)time(NULL));
            monitorp->curbuf += bytes;
            query_network_usage(&monitorp->curbuf, plugin->action);
        }
    }

    return NULL;
}

static int _init(struct libd_plugin *plugin) {
    // alloc here
    struct monitor_pstate *monitorp = 
        (struct monitor_pstate *)realloc(plugin->pstate, sizeof(struct monitor_pstate));
    plugin->pstate = (struct libd_pstate *)monitorp;

#if ENABLE_MONITORTHREAD
    monitorp->interval = 1000;
    int ret = pthread_create(&monitorp->pid, NULL, monitor_loop, (void *)plugin);
    if (ret){
        printf("ERROR; return code from pthread_create() is %d\n", ret);
        return ret;
    }
#endif

    dprintf("init: prepare file handlers");
    // prepare files for memory usage
    monitorp->procf_mem = open("/proc/self/statm", O_RDONLY);
    monitorp->procf_cpu = open("/proc/self/stat", O_RDONLY);

    dprintf("init: setting up initial monitor values...");
    // prepare first timestamp
    char * bufp = monitorp->buf;
    query_cpu_usage(&bufp, monitorp->procf_cpu, monitorp);
    init_buf(monitorp, plugin->action->aid, plugin->action->name);

    curl_global_init(CURL_GLOBAL_ALL);
    dprintf("init: finish");
    return 0;
}

static int _terminate(struct libd_plugin *plugin) {
    struct monitor_pstate *monitorp = 
        (struct monitor_pstate *)(plugin->pstate);

    // Post panding data
    dprintf("terminate:");
    post_stat(monitorp, plugin->action->post_url);
    close(monitorp->procf_cpu);
    close(monitorp->procf_mem);

#if ENABLE_MONITORTHREAD
    pthread_cancel(monitorp->pid);
#endif

    // deallocate ds
    plugin->pstate = (struct libd_pstate *)realloc(plugin->pstate, sizeof(struct libd_pstate));

    curl_global_cleanup();
    return 0;
}

static int _invoke(struct libd_plugin *plugin, int point, void * param) {
    // Currently, we only have the log point
    int bytes;
    long ts;
    struct timespec t;

    struct monitor_pstate *monitorp = 
        (struct monitor_pstate *)(plugin->pstate);

    dprintf("invoke: with point %d", point);

    // get timestamp
    clock_gettime(CLOCK_BOOTTIME, &t);
    ts = t.tv_nsec / 1000000 + t.tv_sec * 1000;

    bytes = sprintf(monitorp->curbuf, "&point_%d=%ld", point, ts);
    monitorp->curbuf += bytes;

    query_cpu_usage(&monitorp->curbuf, monitorp->procf_cpu, monitorp);
    query_memory_usage(&monitorp->curbuf, monitorp->procf_mem);
    query_network_usage(&monitorp->curbuf, plugin->action);

    if (monitorp->curbuf - monitorp->buf >=
            BUFFER_SIZE - BUFFER_THRESHOLD) {
        post_stat(monitorp, plugin->action->post_url);
        init_buf(monitorp, plugin->action->aid, plugin->action->name);
    }

    return 0;
}

struct libd_p monitor_plugin = {
    .init = _init,
    .terminate = _terminate,
    .invoke = _invoke
};


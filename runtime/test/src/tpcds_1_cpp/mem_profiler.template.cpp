#include <iostream>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <string>
#include <vector>
#include <cstring>
#include <execinfo.h>
#include <fcntl.h>
#include <malloc.h>
#include <math.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

// -----------------------------------------------------------------------------
// * Define the TCMallocHook
// -----------------------------------------------------------------------------
// Semaphore
#define SEM_NAME "/mysemaphore"
#define LOG_FILE_NAME_ENV_VAR_NAME "LOG_FILE_NAME"
#define DISABLE_HOOK_ENV_VAR_NAME "DISABLE_HOOK"
//
// The time when the program started.
// The log file to write to.
static FILE * logFile;
const static auto program_start_time = std::chrono::steady_clock::now();
static auto _program_name = std::string(__FILE__);
static auto program_name = _program_name.substr(0, _program_name.find_last_of("."));
static char ** global_argv;
static int global_argc;

// mutex of logfile
sem_t *mutex;

extern "C" {
    typedef void (*MallocHook_NewHook)(const void* ptr, size_t size);
    int MallocHook_AddNewHook(MallocHook_NewHook hook);
    int MallocHook_RemoveNewHook(MallocHook_NewHook hook);

    typedef void (*MallocHook_DeleteHook)(const void* ptr);
    int MallocHook_AddDeleteHook(MallocHook_DeleteHook hook);
    int MallocHook_RemoveDeleteHook(MallocHook_DeleteHook hook);
}   // extern "C"

void NewHook(const void* ptr, size_t size);
void DeleteHook(const void* ptr);


void print_traceback(){
    int n = 256;
    void * array[n];
    n = backtrace(array, n);
    char** symbols = backtrace_symbols(array, n);
    assert(symbols != NULL);

    // Print the backtrace
    for (int i = 2; i < n; i++) { // Note: starting from i = 2 becuase i = 1 is the fprintf line.
        fprintf(logFile, "\"%s\"", symbols[i]);
        if (i != n - 1) {
            fprintf(logFile, ", ");
        }
    }

    // Free the string pointers
    free(symbols);
}

typedef std::chrono::microseconds DurationType_t;

void NewHook(const void* ptr, size_t size)
{
    // Filter less than 4KB  = 4 * 1024
    // 8KB = 8 * 1024
    // if (size <= 4 * 1024){
        // return ;
    // }

    sem_wait(mutex);
    {
        // Disable the hook to avoid infinite recursion
        MallocHook_RemoveNewHook(&NewHook);
        MallocHook_RemoveDeleteHook(&DeleteHook);

        {
            // Get current chrono time.
            auto now = std::chrono::steady_clock::now();
            auto c = std::chrono::duration_cast<DurationType_t>(now - program_start_time).count();
            fprintf(
                logFile,
                "{ \"event\": \"NewHook\", \"ptr\": \"%p\", \"size\": \"%ld\", \"timestamp\": \"%ld\",",
                ptr, size, c
            );
            fprintf(logFile, " \"traceback\": [");
            print_traceback();
            fprintf(logFile, "]}\n");
            fflush(logFile);
        }

        // Re-enable the hook
        MallocHook_AddNewHook(&NewHook);
        MallocHook_AddDeleteHook(&DeleteHook);
    }
    sem_post(mutex);

}

void DeleteHook(const void* ptr)
{
    sem_wait(mutex);
    {
        MallocHook_RemoveNewHook(&NewHook);
        MallocHook_RemoveDeleteHook(&DeleteHook);

        {
            auto now = std::chrono::steady_clock::now();
            auto c = std::chrono::duration_cast<DurationType_t>(now - program_start_time).count();
            fprintf(
                logFile,
                "{ \"event\": \"DeleteHook\", \"ptr\": \"%p\", \"timestamp\": \"%ld\",",
                ptr, c
            );
            fprintf(logFile, " \"traceback\": [");
            print_traceback();
            fprintf(logFile, "]}\n");
            fflush(logFile);
        }

        MallocHook_AddNewHook(&NewHook);
        MallocHook_AddDeleteHook(&DeleteHook);
    }
    sem_post(mutex);

}

void setup_logging(){

    // Add environment variable
    const char * log_file_name = getenv(LOG_FILE_NAME_ENV_VAR_NAME);
    if (log_file_name == nullptr) {
        std::cerr << "Environment variable " << LOG_FILE_NAME_ENV_VAR_NAME << " is not set." << std::endl;
        std::cerr << "Please set it to the log file name." << std::endl;
        std::cerr << "Example Usage: " << LOG_FILE_NAME_ENV_VAR_NAME << "=<log_file_name> " << global_argv[0] << " <other args>" << std::endl;
        exit(1);
    }
    logFile = fopen(log_file_name, "w+");
    if (logFile == nullptr) {
        std::cerr << "Failed to open log file: " << log_file_name << std::endl;
        exit(1);
    }

    mutex = sem_open(SEM_NAME, O_CREAT, 0644, 1);
    if (mutex == SEM_FAILED) {
        perror("unable to create semaphore");
        sem_unlink(SEM_NAME);
        exit(-1);
    }

}

void init_hook()
{
    setup_logging();

    MallocHook_AddNewHook(&NewHook);
    MallocHook_AddDeleteHook(&DeleteHook);

    return ;
}

void destroy_hook()
{
    MallocHook_RemoveNewHook(&NewHook);
    MallocHook_RemoveDeleteHook(&DeleteHook);

    // Close the log file.
    fclose(logFile);
    sem_close(mutex);
    sem_unlink(SEM_NAME);
    return ;
}

int main_(int argc, char * argv[]); // Assume main_ is defined in another scope.


int main(int argc, char * argv[])  {
    global_argv = argv;
    global_argc = argc;;

    const char * _disable_hook = getenv(DISABLE_HOOK_ENV_VAR_NAME);
    bool disable_hook = false;
    if (_disable_hook){
        if (strcmp(_disable_hook, "1") == 0){
            disable_hook = true;
        } else if (strcmp(_disable_hook, "true") == 0){
            disable_hook = true;
        } else if (strcmp(_disable_hook, "True") == 0){
            disable_hook = true;
        } else if (strcmp(_disable_hook, "TRUE") == 0){
            disable_hook = true;
        }
    }
    if (!disable_hook){
        init_hook();
        fprintf(stderr, "tcmalloc hook enabled.\n");
    }

    {
        main_(argc, argv);
    }

    if (!disable_hook){
        destroy_hook();
    }
    
    return 0;
}
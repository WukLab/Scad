#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <malloc.h>
#include <cassert>
#include <execinfo.h> // backtrace
#include <chrono>
#include <unistd.h>
#include <iostream>
#include <malloc.h>
#include <cassert>
#include <execinfo.h> // backtrace
#include <chrono>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <string>
#include <signal.h>
#include <execinfo.h>
#include <unistd.h>
#include <cstdlib>
#include <fcntl.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <sys/wait.h>

#include "Python.h"

// required by numpy CAPI
#define PY_ARRAY_UNIQUE_SYMBOL private_NUMPY_ARRAY_API
// required by numpy 1.8+
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include "numpy/arrayobject.h"

#include "schema.h"
#include "utils.h"

// Semaphore
#define SEM_NAME "/mysemaphore"
#define LOG_FILE_NAME_ENV_VAR_NAME "LOG_FILE_NAME"
#define DISABLE_HOOK_ENV_VAR_NAME "DISABLE_HOOK"
// -----------------------------------------------------------------------------
// * Define the TCMallocHook
// -----------------------------------------------------------------------------
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
    // / 8KB = 8 * 1024
    if (size <= 4 * 1024){
        return ;
    }

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

    // Setup the log file.
    // Use the program name and the current time to create a unique log file name.
    auto now = std::chrono::system_clock::now();
    auto now_c = std::chrono::system_clock::to_time_t(now);
    char time_str[100];
    strftime(time_str, sizeof(time_str), "%Y%m%d_%H%M%S", std::localtime(&now_c));

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

// -----------------------------------------------------------------------------




int _main(int argc, char * argv[]) {
    wchar_t *program = Py_DecodeLocale(argv[0], NULL);
    Py_SetProgramName(program);  /* optional but recommended */
    Py_Initialize();
    import_array();

    // imports
    PyObject* m_numpy   = PyImport_Import(PyUnicode_FromString("numpy"));
    PyObject* m_request = PyImport_Import(PyUnicode_FromString("urllib.request"));
    PyObject* m_join    = PyImport_Import(PyUnicode_FromString("npjoin.join"));
    PyObject* m_rfn     = PyImport_Import(PyUnicode_FromString("numpy.lib.recfunctions"));
    PyObject* m_npg     = PyImport_Import(PyUnicode_FromString("numpy_groupies"));
    CHECKO(m_join);

    // functions
    PyObject* f_genfromtxt = PyObject_GetAttrString(m_numpy, "genfromtxt");
    PyObject *f_joinon = PyObject_GetAttrString(m_join, "join_on_table_float32");

    //////////
    // step1
    //////////
    // TODO: insert DEREFs, rename vars

    // building the schema
    PyObject* o_csv = PyObject_CallMethod(m_request, "urlopen",
        "s", "http://localhost:8123/date_dim.csv");

    PyObject* o_dtype = PyObject_CallMethod(m_numpy, "dtype",
            SCHEMA_FMT(STEP1_SCHEMA_SIZE), STEP1_SCHEMA);
    PyObject* p_dict = Py_BuildValue("{s:O, s:s, s:O}",
        "fname", o_csv, "delimiter", "|", "dtype", o_dtype);

    PyObject* o_df = PyObject_Call(f_genfromtxt, PyTuple_New(0), p_dict);
    CHECKO(o_df);

    // projection
    PyObject *o_tmp  = PyObject_CallMethod(o_df,   "__getitem__", "s", "d_year");
    CHECKO(o_tmp);
    PyObject *o_tmp2 = PyObject_CallMethod(o_tmp,  "__eq__", "i", 2000);
    CHECKO(o_tmp2);
    Py_DECREF(o_tmp);
    PyObject *o_tmp3 = PyObject_CallMethod(o_df,   "__getitem__", "O", o_tmp2);
    CHECKO(o_tmp3);
    PyObject *o_proj = PyObject_CallMethod(o_tmp3, "__getitem__", "[s]", "d_date_sk");
    CHECKO(o_proj);
    Py_DECREF(o_tmp2);

    // Do the repack in C
    npy_intp stride0 = PyArray_STRIDE((PyArrayObject *)o_proj, 0);
    npy_intp dim0 = PyArray_DIM((PyArrayObject *)o_proj, 0);
    char * data = (char *)PyArray_BYTES((PyArrayObject *)o_proj);

    npy_float * buffer = (npy_float *)malloc(dim0 * sizeof(npy_float));
    for (int i = 0; i < dim0; i ++, data += stride0)
        buffer[i] = *(npy_float *)data;
    Py_DECREF(o_proj);

    PyObject *o_df1 = PyArray_SimpleNewFromData(1, &dim0, NPY_FLOAT, buffer);

    //////////
    // step2
    //////////
    o_csv = PyObject_CallMethod(m_request, "urlopen",
        "s", "http://localhost:8123/store_returns.csv");

    o_dtype = PyObject_CallMethod(m_numpy, "dtype",
            SCHEMA_FMT(STEP2_SCHEMA_SIZE), STEP2_SCHEMA);
    p_dict = Py_BuildValue("{s:O, s:s, s:O}",
        "fname", o_csv, "delimiter", "|", "dtype", o_dtype);

    PyObject *o_df2 = PyObject_Call(f_genfromtxt, PyTuple_New(0), p_dict);

    //////////
    // step3
    //////////
    // join the index
    PyObject *o_df3;
    {
        PyObject *o_tup = PyObject_CallMethod(m_join, "prepare_join_float32", "O", o_df1);
        _PyTuple_Resize(&o_tup, 5);

        PyObject *o_df2_slice = PyObject_CallMethod(o_df2, "__getitem__",
                    "s", "sr_returned_date_sk");
        PyTuple_SetItem(o_tup, 4, o_df2_slice);
        
        PyObject *o_ret_tup = PyObject_CallObject(f_joinon, o_tup);

        PyObject *df2_idx = PyTuple_GetItem(o_ret_tup, 1);
        npy_intp *df2_idx_buf = (npy_intp *)PyArray_DATA((PyArrayObject *)df2_idx);

        // create buffer for df3
        PyArray_Descr *df3_descr;
        PyObject *df3_tup = Py_BuildValue(SCHEMA_FMT(DF3_SCHEMA_SIZE), DF3_SCHEMA);
        PyArray_DescrConverter(df3_tup, &df3_descr);
        npy_intp df3_len = PyArray_DIM((PyArrayObject *)df2_idx, 0);

        float *buf_df3 = (float *) malloc(df3_len * df3_descr->elsize);
        // Actual datacopy for join
        char *buf_df2 = PyArray_BYTES((PyArrayObject *)o_df2);
        npy_intp df2_elsize = PyArray_ITEMSIZE((PyArrayObject *)o_df2);

        const int fields_df2 = 3;
        const int offset_df2[] = { 3, 7, 11 };
        float* bufp = buf_df3;
        for (npy_intp i = 0; i < df3_len; i++, bufp += fields_df2) {
            float *line = (float *)(buf_df2 + df2_idx_buf[i] * df2_elsize);
            // TODO: unroll?
            for (int j = 0; j < fields_df2; j++)
                bufp[j] = line[offset_df2[j]];
        }
        o_df3 = PyArray_NewFromDescr(
          &PyArray_Type, df3_descr, 1, &df3_len, NULL, buf_df3, 0, NULL);
    }
    PRINTO(o_df3);

    
    //////////
    // step4
    //////////
    // TODO: numpy capis?
    // PyObject *o_df4_t1 = PyObject_CallMethod(o_df3, "__getitem__", "s", "sr_customer_sk");
    // PyObject *o_df4_t2 = PyObject_CallMethod(m_numpy, "isnan", "O", o_df4_t1);
    // PyObject *o_df4_t3 = PyObject_CallMethod(o_df4_t2, "__invert__");

    // PyObject *o_df4_t4 = PyObject_CallMethod(o_df3, "__getitem__", "s", "st_store_sk");
    // PyObject *o_df4_t5 = PyObject_CallMethod(m_numpy, "isnan", "O", o_df4_t2);
    // PyObject *o_df4_t6 = PyObject_CallMethod(o_df4_t5, "__invert__");


    Py_Finalize();
    return 0;
}


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
        _main(argc, argv);
    }

    if (!disable_hook){
        destroy_hook();
    }
    
    return 0;
}
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>

#include "Python.h"

#ifndef PORT
    #define PORT 8456
#endif
#define XSTR(x) STR(x)
#define STR(x) #x

// required by numpy CAPI
#define PY_ARRAY_UNIQUE_SYMBOL private_NUMPY_ARRAY_API
// required by numpy 1.8+
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include "numpy/arrayobject.h"

#include "schema.h"
#include "utils.h"

static inline double time_diff(struct timespec s, struct timespec e) {
    return (e.tv_sec - s.tv_sec) + 1.0e-9 * (e.tv_nsec - s.tv_nsec);
}

int main(int argc, const char * argv[]) {
    wchar_t *program = Py_DecodeLocale(argv[0], NULL);
    Py_SetProgramName(program);  /* optional but recommended */
    Py_Initialize();
    import_array();

    struct timespec stime, etime;

    // imports
    PyObject* m_numpy   = PyImport_Import(PyUnicode_FromString("numpy"));
    PyObject* m_request = PyImport_Import(PyUnicode_FromString("urllib.request"));
    PyObject* m_join    = PyImport_Import(PyUnicode_FromString("npjoin.join"));
    PyObject* m_rfn     = PyImport_Import(PyUnicode_FromString("numpy.lib.recfunctions"));
    PyObject* m_npg     = PyImport_Import(PyUnicode_FromString("numpy_groupies"));

    // functions
    PyObject* f_genfromtxt = PyObject_GetAttrString(m_numpy, "genfromtxt");
    PyObject *f_unique = PyObject_GetAttrString(m_numpy, "unique");
    PyObject *f_joinon = PyObject_GetAttrString(m_join, "join_on_table_float32");
    PyObject *f_merge = PyObject_GetAttrString(m_rfn, "merge_arrays");
    PyObject *f_agg = PyObject_GetAttrString(m_npg, "aggregate");

    CHECKO(f_merge);
    CHECKO(f_joinon);
    CHECKO(f_agg);

    //////////
    // step1
    //////////
    // TODO: insert DEREFs, rename vars

    // building the schema
    PyObject *o_df1;
    char *df1_buf;
    {
        PyObject* o_csv = PyObject_CallMethod(m_request, "urlopen",
            "s", "http://localhost:" XSTR(PORT) "/date_dim.csv");

        PyObject* o_dtype = PyObject_CallMethod(m_numpy, "dtype",
                SCHEMA_FMT(STEP1_SCHEMA_SIZE), STEP1_SCHEMA);
        PyObject* p_dict = Py_BuildValue("{s:O, s:s, s:O}",
            "fname", o_csv, "delimiter", "|", "dtype", o_dtype);

        PyObject* o_df = PyObject_Call(f_genfromtxt, PyTuple_New(0), p_dict);

        clock_gettime(CLOCK_REALTIME, &stime);
        // projection
        PyObject *o_tmp  = PyObject_CallMethod(o_df,   "__getitem__", "s", "d_year");
        PyObject *o_tmp2 = PyObject_CallMethod(o_tmp,  "__eq__", "i", 2000);
        PyObject *o_tmp3 = PyObject_CallMethod(o_df,   "__getitem__", "O", o_tmp2);
        PyObject *o_proj = PyObject_CallMethod(o_tmp3, "__getitem__", "[s]", "d_date_sk");

        // Do the repack in C
        npy_intp stride0 = PyArray_STRIDE((PyArrayObject *)o_proj, 0);
        npy_intp dim0 = PyArray_DIM((PyArrayObject *)o_proj, 0);
        char * data = (char *)PyArray_BYTES((PyArrayObject *)o_proj);

        npy_float * buffer = (npy_float *)malloc(dim0 * sizeof(npy_float));
        for (int i = 0; i < dim0; i ++, data += stride0)
            buffer[i] = *(npy_float *)data;
        df1_buf = (char *)buffer;
        Py_DECREF(o_df);

        o_df1 = PyArray_SimpleNewFromData(1, &dim0, NPY_FLOAT, buffer);
    }

    clock_gettime(CLOCK_REALTIME, &etime);
    printf("Step1: %lf\n", time_diff(stime, etime));

    //////////
    // step2
    //////////

    PyObject *o_df2;
    WORKER(1) {
        PyObject *o_csv = PyObject_CallMethod(m_request, "urlopen",
            "s", "http://localhost:" XSTR(PORT) "/store_returns.csv");

        PyObject *o_dtype = PyObject_CallMethod(m_numpy, "dtype",
                SCHEMA_FMT(STEP2_SCHEMA_SIZE), STEP2_SCHEMA);
        PyObject *p_dict = Py_BuildValue("{s:O, s:s, s:O}",
            "fname", o_csv, "delimiter", "|", "dtype", o_dtype);

        o_df2 = PyObject_Call(f_genfromtxt, PyTuple_New(0), p_dict);
    }

    clock_gettime(CLOCK_REALTIME, &stime);
    clock_gettime(CLOCK_REALTIME, &etime);
    printf("Step2: %lf\n", time_diff(stime, etime));

    //////////
    // step3
    //////////
    // join the index
    PyObject *o_df3;
    float *buf_df3;
    npy_intp df3_len;

    clock_gettime(CLOCK_REALTIME, &stime);
    WORKER(1) {
        PyObject *o_tup = PyObject_CallMethod(m_join, "prepare_join_float32", "O", o_df1);
        Py_DECREF(o_df1);
        free(df1_buf);

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
        df3_len = PyArray_DIM((PyArrayObject *)df2_idx, 0);

        buf_df3 = (float *)malloc(df3_len * df3_descr->elsize);
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
        Py_DECREF(o_df2);

        o_df3 = PyArray_NewFromDescr(
          &PyArray_Type, df3_descr, 1, &df3_len, NULL, buf_df3, 0, NULL);
    }
    clock_gettime(CLOCK_REALTIME, &etime);
    printf("Step3: %lf\n", time_diff(stime, etime));
    
    //////////
    // step4
    //////////
    PyObject *o_df4;
    WORKER(1) {
        // build predicate
        PyObject *arr_bool = PyArray_SimpleNew(1, &df3_len, NPY_BOOL);
        npy_bool *buf_bool = (npy_bool *)PyArray_DATA((PyArrayObject *)arr_bool);

        float *bufp = buf_df3;
        for (int i = 0; i < df3_len; i++, bufp += 3)
            buf_bool[i] = !isnan(bufp[0]) && !isnan(bufp[1]);

        PyObject *o_filtered = PyObject_CallMethod(o_df3, "__getitem__",
                "O", arr_bool);

        PyObject* o_cols = PyObject_CallMethod(o_filtered, "__getitem__",
                "[s,s]", "sr_customer_sk", "sr_store_sk");
        PyObject* d_uniq = Py_BuildValue("{s:O}", "return_inverse", Py_True);
        PyObject* t_uniq = PyObject_Call(f_unique, PyTuple_Pack(1, o_cols), d_uniq);

        PyObject *o_uniques  = PyTuple_GetItem(t_uniq, 0);
        PyObject *o_reverses = PyTuple_GetItem(t_uniq, 1);
        
        // get groups
        PyObject* o_proj = PyObject_CallMethod(o_filtered, "__getitem__",
                "s", "sr_return_amt");
        PyObject* d_agg = Py_BuildValue("{s:s}", "func", "sum");
        PyObject* o_groups = PyObject_Call(f_agg, PyTuple_Pack(2, o_reverses, o_proj), d_agg);

        // aggregate
        PyObject* d_merge = Py_BuildValue("{s:[O,O], s:O, s:O}",
            "seqarrays", o_uniques, o_groups, "flatten", Py_True, "usemask", Py_False);
        o_df4 = PyObject_Call(f_merge, PyTuple_New(0), d_merge);

        // free df3
        Py_DECREF(o_df3);
        free(buf_df3);
    }

    //////////
    // step5
    //////////
    PyObject *o_df5;
    npy_intp df5_len;
    char * df5_buf;
    WORKER(1) {
        PyObject* o_csv = PyObject_CallMethod(m_request, "urlopen",
            "s", "http://localhost:" XSTR(PORT) "/customer.csv");

        PyObject* o_dtype = PyObject_CallMethod(m_numpy, "dtype",
                SCHEMA_FMT(DF5_SCHEMA_SIZE), DF5_SCHEMA);
        PyObject* p_dict = Py_BuildValue("{s:O, s:s, s:O}",
            "fname", o_csv, "delimiter", "|", "dtype", o_dtype);

        PyObject* o_df = PyObject_Call(f_genfromtxt, PyTuple_New(0), p_dict);

        // Do the repack in C
        df5_len = PyArray_DIM((PyArrayObject *)o_df, 0);
        npy_intp df_stride = PyArray_STRIDE((PyArrayObject *)o_df, 0);

        PyArray_Descr *df5_descr;
        PyObject *df5_tup = Py_BuildValue(
                "[(s,s),(s,s)]", "c_customer_sk", "f4", "c_customer_id", "S16");
        PyArray_DescrConverter(df5_tup, &df5_descr);
        npy_intp df5_elsize = df5_descr->elsize;

        df5_buf = (char *)malloc(df5_len * df5_descr->elsize);
        o_df5 = PyArray_NewFromDescr(
          &PyArray_Type, df5_descr, 1, &df5_len, NULL, df5_buf, 0, NULL);

        char *src_bufp = PyArray_BYTES((PyArrayObject *)o_df);
        char *dst_bufp = df5_buf;
        for (int i = 0; i < df5_len;
                        i ++, src_bufp += df_stride, dst_bufp += df5_elsize)
            memcpy(dst_bufp, src_bufp, df5_elsize);
    }

    //////////
    // step6
    //////////
    PyObject *o_df6;
    char *df6_buf;
    WORKER(1) {
        PyObject *s_df5 = PyObject_CallMethod(o_df5, "__getitem__",
            "s", "c_customer_sk");

        PyObject *t_prep = PyObject_CallMethod(m_join, "prepare_join_float32", "O", s_df5);
        _PyTuple_Resize(&t_prep, 5);

        // index the first column
        PyObject *s_df4 = PyObject_CallMethod(o_df4, "__getitem__",
             "s", "sr_customer_sk");
        PyTuple_SetItem(t_prep, 4, s_df4);
        
        PyObject *t_join = PyObject_CallObject(f_joinon, t_prep);

        PyObject *j_df5_idx = PyTuple_GetItem(t_join, 0);
        PyObject *j_df4_idx = PyTuple_GetItem(t_join, 1);
        npy_intp *buf_df4_idx = (npy_intp *) PyArray_DATA((PyArrayObject *)j_df4_idx);
        npy_intp *buf_df5_idx = (npy_intp *) PyArray_DATA((PyArrayObject *)j_df5_idx);
        
        // copy the merged project
        PyArray_Descr *df6_descr;
        PyObject *df6_tup = Py_BuildValue(SCHEMA_FMT(DF6_SCHEMA_SIZE), DF6_SCHEMA);
        PyArray_DescrConverter(df6_tup, &df6_descr);
        npy_intp df6_len = PyArray_DIM((PyArrayObject *)j_df4_idx, 0);

        npy_intp df6_elsize = df6_descr->elsize;
        df6_buf = (char *)malloc(df6_len * df6_elsize);
        // Actual datacopy for join
        char *df4_buf = PyArray_BYTES((PyArrayObject *)o_df4);
        char *df5_buf = PyArray_BYTES((PyArrayObject *)o_df5);

        npy_intp df4_elsize = PyArray_ITEMSIZE((PyArrayObject *)o_df4);
        npy_intp df5_elsize = PyArray_ITEMSIZE((PyArrayObject *)o_df5);

        // const int fields_df4 = 1;
        // const int offset_df4[] = { 0 };
        // const int bytes_df4[] = { 12 };

        // const int fields_df5 = 1;
        // const int offset_df5[] = { 4 };
        // const int bytes_df5[] = { 16 };

        char* bufp = df6_buf;
        for (npy_intp i = 0; i < df6_len; i++, bufp += df6_elsize) {
            char *bufp_df4 = df4_buf + buf_df4_idx[i] * df4_elsize;
            memcpy(bufp, bufp_df4, 12);
            char *bufp_df5 = df5_buf + buf_df5_idx[i] * df5_elsize;
            memcpy(bufp + 12, bufp_df5 + 4, 16);
        }

        Py_DECREF(o_df4);
        Py_DECREF(o_df5);
        free(df5_buf);

        o_df6 = PyArray_NewFromDescr(
          &PyArray_Type, df6_descr, 1, &df6_len, NULL, df6_buf, 0, NULL);
    }

    //////////
    // step7
    //////////
    PyObject *o_df7;
    WORKER(1) {
        PyObject* o_csv = PyObject_CallMethod(m_request, "urlopen",
            "s", "http://localhost:" XSTR(PORT) "/store.csv");

        // TODO: why alignment == error?
        PyObject* o_dtype = PyObject_CallMethod(m_numpy, "dtype",
                SCHEMA_FMT(DF7_SCHEMA_SIZE), DF7_SCHEMA);
        PyObject* p_dict = Py_BuildValue("{s:O, s:s, s:O}",
            "fname", o_csv, "delimiter", "|", "dtype", o_dtype);

        PyObject* o_df = PyObject_Call(f_genfromtxt, PyTuple_New(0), p_dict);
        // since this table is small, skip over the projection!
        o_df7 = o_df;

        // npy_intp df_len = PyArray_DIM((PyArrayObject *)o_df, 0);
        // npy_intp df_stride = PyArray_STRIDE((PyArrayObject *)o_df, 0);
        // 
        // PyArray_Descr *descr = PyArray_DESCR((PyArrayObject *)o_df);
        // PRINTO(descr->fields);

        // PyArray_Descr *df5_descr;
        // PyObject *df5_tup = Py_BuildValue(
        //         "[(s,s),(s,s)]", "c_customer_sk", "f4", "c_customer_id", "S16");
        // PyArray_DescrConverter(df5_tup, &df5_descr);
        // npy_intp df5_elsize = df5_descr->elsize;

        // df5_buf = (char *)malloc(df5_len * df5_descr->elsize);
        // o_df5 = PyArray_NewFromDescr(
        //   &PyArray_Type, df5_descr, 1, &df5_len, NULL, df5_buf, 0, NULL);

        // char *src_bufp = PyArray_BYTES((PyArrayObject *)o_df);
        // char *dst_bufp = df5_buf;
        // for (int i = 0; i < df5_len;
        //                 i ++, src_bufp += df_stride, dst_bufp += df5_elsize)
        //     memcpy(dst_bufp, src_bufp, df5_elsize);
    }

    //////////
    // step8 - use import?
    //////////
    Py_DECREF(o_df6);
    Py_DECREF(o_df7);
    free(df6_buf);

    Py_Finalize();
    return 0;
}
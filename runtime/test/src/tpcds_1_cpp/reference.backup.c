#include <stdio.h>
#include <stdlib.h>

#include "Python.h"

// required by numpy CAPI
#define PY_ARRAY_UNIQUE_SYMBOL private_NUMPY_ARRAY_API
// required by numpy 1.8+
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include "numpy/arrayobject.h"

#include "schema.h"
#include "utils.h"

int main(int argc, const char * argv[]) {
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

        float *buf_df3 = malloc(df3_len * df3_descr->elsize);
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

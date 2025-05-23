cimport cython
from cpython.mem cimport (
    PyMem_Free,
    PyMem_Malloc,
)
from cpython.ref cimport (
    Py_INCREF,
    PyObject,
)
from libc.stdlib cimport (
    free,
    malloc,
)

import numpy as np

cimport numpy as cnp
from numpy cimport (
    float64_t,
    ndarray,
    uint8_t,
    uint32_t,
)
from numpy.math cimport NAN

cnp.import_array()

#######
# inline util
cdef extern from "numpy/npy_common.h":
    int64_t NPY_MIN_INT64

cdef inline int64_t get_nat():
    return NPY_MIN_INT64

cdef class C_NAType:
    pass

cdef C_NAType C_NA
#
########

from npjoin.khash cimport (
    KHASH_TRACE_DOMAIN,
    are_equivalent_float32_t,
    are_equivalent_float64_t,
    are_equivalent_khcomplex64_t,
    are_equivalent_khcomplex128_t,
    kh_needed_n_buckets,
    kh_str_t,
    khcomplex64_t,
    khcomplex128_t,
    khiter_t,
)

# Check null
cpdef bint checknull(object val):
    """
    Return boolean describing of the input is NA-like, defined here as any
    of:
     - None
     - nan
     - NaT
     - NA
    Parameters
    ----------
    val : object
    Returns
    -------
    bool
    Notes
    -----
    The difference between `checknull` and `checknull_old` is that `checknull`
    does *not* consider INF or NEGINF to be NA.
    """
    return val is C_NA

# start of python

def get_hashtable_trace_domain():
    return KHASH_TRACE_DOMAIN

cdef int64_t NPY_NAT = get_nat()
SIZE_HINT_LIMIT = (1 << 20) + 7


cdef Py_ssize_t _INIT_VEC_CAP = 128

include "hashtable_class_helper.pxi"
include "hashtable_func_helper.pxi"

cdef class Factorizer:
    cdef public:
        PyObjectHashTable table
        ObjectVector uniques
        Py_ssize_t count

    def __init__(self, size_hint: int):
        self.table = PyObjectHashTable(size_hint)
        self.uniques = ObjectVector()
        self.count = 0

    def get_count(self) -> int:
        return self.count

    def factorize(
        self, ndarray[object] values, sort=False, na_sentinel=-1, na_value=None
    ) -> np.ndarray:
        """

        Returns
        -------
        np.ndarray[np.intp]

        Examples
        --------
        Factorize values with nans replaced by na_sentinel

        >>> factorize(np.array([1,2,np.nan], dtype='O'), na_sentinel=20)
        array([ 0,  1, 20])
        """
        cdef:
            ndarray[intp_t] labels

        if self.uniques.external_view_exists:
            uniques = ObjectVector()
            uniques.extend(self.uniques.to_array())
            self.uniques = uniques
        labels = self.table.get_labels(values, self.uniques,
                                       self.count, na_sentinel, na_value)
        mask = (labels == na_sentinel)
        # sort on
        if sort:
            sorter = self.uniques.to_array().argsort()
            reverse_indexer = np.empty(len(sorter), dtype=np.intp)
            reverse_indexer.put(sorter, np.arange(len(sorter)))
            labels = reverse_indexer.take(labels, mode='clip')
            labels[mask] = na_sentinel
        self.count = len(self.uniques)
        return labels

    def unique(self, ndarray[object] values):
        # just for fun
        return self.table.unique(values)


cdef class Int64Factorizer:
    cdef public:
        Int64HashTable table
        Int64Vector uniques
        Py_ssize_t count

    def __init__(self, size_hint: int):
        self.table = Int64HashTable(size_hint)
        self.uniques = Int64Vector()
        self.count = 0

    def get_count(self) -> int:
        return self.count

    def factorize(self, const int64_t[:] values, sort=False,
                  na_sentinel=-1, na_value=None) -> np.ndarray:
        """
        Returns
        -------
        ndarray[intp_t]

        Examples
        --------
        Factorize values with nans replaced by na_sentinel

        >>> factorize(np.array([1,2,np.nan], dtype='O'), na_sentinel=20)
        array([ 0,  1, 20])
        """
        cdef:
            ndarray[intp_t] labels

        if self.uniques.external_view_exists:
            uniques = Int64Vector()
            uniques.extend(self.uniques.to_array())
            self.uniques = uniques
        labels = self.table.get_labels(values, self.uniques,
                                       self.count, na_sentinel,
                                       na_value=na_value)

        # sort on
        if sort:
            sorter = self.uniques.to_array().argsort()
            reverse_indexer = np.empty(len(sorter), dtype=np.intp)
            reverse_indexer.put(sorter, np.arange(len(sorter)))

            labels = reverse_indexer.take(labels)

        self.count = len(self.uniques)
        return labels


@cython.wraparound(False)
@cython.boundscheck(False)
def unique_label_indices(const int64_t[:] labels):
    """
    Indices of the first occurrences of the unique labels
    *excluding* -1. equivalent to:
        np.unique(labels, return_index=True)[1]
    """
    cdef:
        int ret = 0
        Py_ssize_t i, n = len(labels)
        kh_int64_t *table = kh_init_int64()
        Int64Vector idx = Int64Vector()
        ndarray[int64_t, ndim=1] arr
        Int64VectorData *ud = idx.data

    kh_resize_int64(table, min(kh_needed_n_buckets(n), SIZE_HINT_LIMIT))

    with nogil:
        for i in range(n):
            kh_put_int64(table, labels[i], &ret)
            if ret != 0:
                if needs_resize(ud):
                    with gil:
                        idx.resize()
                append_data_int64(ud, i)

    kh_destroy_int64(table)

    arr = idx.to_array()
    arr = arr[np.asarray(labels)[arr].argsort()]

    return arr[1:] if arr.size != 0 and labels[arr[0]] == -1 else arr

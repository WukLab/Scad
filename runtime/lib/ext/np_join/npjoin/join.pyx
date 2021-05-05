import cython
from cython import Py_ssize_t

cnp.import_array()

import numpy as np
cimport numpy as cnp
from numpy cimport (
    float32_t,
    float64_t,
    int8_t,
    int16_t,
    int32_t,
    int64_t,
    intp_t,
    ndarray,
    uint8_t,
    uint16_t,
    uint32_t,
    uint64_t,
)

from npjoin.hashtable cimport Float32HashTable, Float32Vector
from npjoin.khash cimport lookup3

def merge_dtypes(ndarray left, ndarray right,
        list left_fields, list right_fields):
    left_dtypes = [(name, t) for name, t
                             in left.dtype.descr
                             if name in left_fields]
    right_dtypes = [(name, t) for name, t
                             in right.dtype.descr
                             if name in right_fields]
    return np.dtype(left_dtypes + right_dtypes)

# simply merge two array, do not repack.
def structured_array_merge(
    uint8_t [::1] buf,
    ndarray left, ndarray right,
    ndarray[intp_t] left_indexer, ndarray[intp_t] right_indexer,
    list left_fields, list right_fields):

    cdef:
        Py_ssize_t size

    # get new dtype
    merged_dtypes = merge_dtypes(left, right, left_fields, right_fields)
    size = len(left_indexer) * merged_dtypes.itemsize
    # allocate new array on buffer
    joined = np.asarray(buf[:size]).view(dtype = merged_dtypes)
    # print('merged dtype', merged_dtypes, 'item', merged_dtypes.itemsize, 'array', joined.shape)

    # assign col by col, using numpy
    # TODO: better copy algorithm
    # see https://stackoverflow.com/questions/5355744/numpy-joining-structured-arrays
    for f in left_fields:
        joined[f] = left[f][left_indexer]
    for f in right_fields:
        joined[f] = right[f][right_indexer]

    # copy from arries
    return joined

# perform left biased join
def join_on_table_float32(
    Float32HashTable hashtable,
    const intp_t[::1] left_sorter,
    const intp_t[::1] left_counter,
    uint64_t left_groups,
    ndarray[float32_t] right
    ):

    cdef:
        ndarray[intp_t] right_factor

    right_factor = hashtable.lookup(right)
    return join_on(left_groups, left_sorter, left_counter, right_factor)

def prepare_join_float32(
    ndarray[float32_t] left
    ):
    cdef:
        int64_t size
        int64_t unique_size
        Float32HashTable hashtable
        ndarray[intp_t] left_index
        Float32Vector left_unique

    size = len(left)
    # TODO: check this size
    hashtable = Float32HashTable(size)
    # TODO: na?
    left_unique, left_index = hashtable.factorize(left)
    unique_size = len(left_unique)
    left_sorter, left_count = groupsort_indexer(left_index, unique_size)
    return hashtable, left_sorter, left_count, unique_size

@cython.boundscheck(False)
def join_on(
    int64_t ngroups,
    const intp_t[::1] left_sorter,
    const intp_t[::1] left_count,
    const intp_t[::1] right):

    cdef:
        Py_ssize_t i, j, k, count = 0
        ndarray[intp_t] right_sorter
        ndarray[intp_t] right_count
        ndarray[intp_t] left_indexer, right_indexer
        intp_t lc, rc
        Py_ssize_t loc, left_pos = 0, right_pos = 0, position = 0
        Py_ssize_t offset

    # get indexer from existing table
    right_sorter, right_count = groupsort_indexer(right, ngroups)
    with nogil:
        for i in range(1, ngroups + 1):
            lc = left_count[i]
            rc = right_count[i]
            if rc > 0 and lc > 0:
                count += lc * rc

    # start joining, build on left
    left_pos = left_count[0]
    right_pos = right_count[0]

    # join result indexer: pos -> pos in indexer
    left_indexer = np.empty(count, dtype=np.intp)
    right_indexer = np.empty(count, dtype=np.intp)

    with nogil:
        for i in range(1, ngroups + 1):
            lc = left_count[i]
            rc = right_count[i]

            if rc > 0 and lc > 0:
                for j in range(lc):
                    offset = position + j * rc
                    for k in range(rc):
                        left_indexer[offset + k] = left_pos + j
                        right_indexer[offset + k] = right_pos + k
                position += lc * rc
            left_pos += lc
            right_pos += rc

    return (deindex(left_sorter, left_indexer),
            deindex(right_sorter, right_indexer))

# partition function
@cython.wraparound(False)
@cython.boundscheck(False)
def partition_on_float32(ndarray[float32_t] values, Py_ssize_t num_bins):
    cdef:
        Py_ssize_t i, size
        ndarray[uint32_t] view
        ndarray[intp_t] bins
        ndarray[intp_t] sorter, groups, sumgroups

    view = values.view(dtype = np.uint32)
    size = view.shape[0]
    bins = np.empty(size, dtype=np.intp)
    with nogil:
        for i in range(0, size):
            bins[i] = lookup3(view[i], 0, 0) % num_bins

    # we need to deindex on this result
    sorter, groups = groupsort_indexer(bins, num_bins)
    sumgroups = np.cumsum(groups)
    return sorter, sumgroups

@cython.wraparound(False)
@cython.boundscheck(False)
def partition_on_float32_pair(ndarray[float32_t] values, ndarray[float32_t] values2, Py_ssize_t num_bins):
    cdef:
        Py_ssize_t i, size
        ndarray[uint32_t] view, view2
        ndarray[intp_t] bins
        ndarray[intp_t] sorter, groups, sumgroups

    view = values.view(dtype = np.uint32)
    view2 = values2.view(dtype = np.uint32)
    size = view.shape[0]
    bins = np.empty(size, dtype=np.intp)
    with nogil:
        for i in range(0, size):
            bins[i] = lookup3(view[i], view2[i], 0) % num_bins

    # we need to deindex on this result
    sorter, groups = groupsort_indexer(bins, num_bins)
    sumgroups = np.cumsum(groups)
    return sorter, sumgroups

# helper functions
# take values from indexer
# TODO: in place?
@cython.wraparound(False)
@cython.boundscheck(False)
def deindex(
    const intp_t[:] values,
    const intp_t[:] indexer,
):
    cdef:
        Py_ssize_t i, n, idx
        intp_t fv
        ndarray[intp_t] out

    n = indexer.shape[0]
    out = np.empty(n, dtype=np.intp)
    fv = -1

    with nogil:
        for i in range(n):
            idx = indexer[i]
            out[i] = values[idx]
    return out

@cython.boundscheck(False)
@cython.wraparound(False)
def groupsort_indexer(const intp_t[:] index, Py_ssize_t ngroups):
    """
    Compute a 1-d indexer.

    The indexer is an ordering of the passed index,
    ordered by the groups.

    Parameters
    ----------
    index: np.ndarray[np.intp]
        Mappings from group -> position.
        index[position] = group
    ngroups: int64
        Number of groups.

    Returns
    -------
    ndarray[intp_t, ndim=1]
        Indexer
    ndarray[intp_t, ndim=1]
        Group Counts

    Notes
    -----
    This is a reverse of the label factorization process.
    """
    cdef:
        Py_ssize_t i, loc, label, n
        ndarray[intp_t] indexer, where, counts

    counts = np.zeros(ngroups + 1, dtype=np.intp)
    n = len(index)
    indexer = np.zeros(n, dtype=np.intp)
    where = np.zeros(ngroups + 1, dtype=np.intp)

    with nogil:

        # count group sizes, location 0 for NA
        for i in range(n):
            counts[index[i] + 1] += 1

        # mark the start of each contiguous group of like-indexed data
        # where: group -> start pos
        for i in range(1, ngroups + 1):
            where[i] = where[i - 1] + counts[i - 1]

        # this is our indexer
        for i in range(n):
            label = index[i] + 1
            # for every element, index[new_index] = i
            indexer[where[label]] = i
            where[label] += 1

    return indexer, counts

# def partition_float32(ndarray[float32] key): -> ndarray[intp]:
    

import cython
from cython import Py_ssize_t

cnp.import_array()

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

from npjoin.hashtable cimport Float32HashTable

# simply merge two array, do not repack.
def structured_array_merge(
    char [:] buf,
    ndarray left, ndarray right,
    intp_t [::1] left_indexer, intp_t [::1] right_indexer,
    list left_fields, list right_fields):

    # get new dtype

    # allocate new array on buffer
    # TODO: buf size
    joined = np.asarray(buf, dtype = merged_dtype)

    # assign col by col, using numpy
    # TODO: better copy algorithm
    joined[left_fields] = left[left_fields][left_indexer]
    joined[right_fields] = right[left_fields][right_indexer]

    # copy from arries
    return joined

# perform left biased join
def join_on_table_float32(
    Float32HashTable hashtable,
    const intp_t[::1] left_sorter,
    const intp_t[::1] left_counter,
    const intp_t[::1] left_groups,
    const ndarray[float32_t] right
    ):

    cdef:
        ndarray[intp_t] right_factor

    right_factor = hashtable.lookup(right)
    return join_on(left_groups, left_sorter, left_counter, right_factor)
    

def prepare_join_float32(
    const ndarray[:] left
    ):
    cdef:
        int64_t size
        int64_t unique_size
        Float32HashTable hashtable
        ndarray[intp_t] left_index
        ndarray[float32] left_unique

    size = len(left)
    hashtable = HashTable(size)
    # TODO: na?
    left_unique, left_index = hashtable.factorize(left)
    unique_size = len(left_unique)
    left_sorter, left_count = groupsort_indexer(left_index, unique_size)
    return hashtable, left_sorter, left_count, unique_size

@cython.boundscheck(False)
@cython.wraparound(False)
def join_on(
    int64_t ngroup,
    const intp_t[:] left_sorter,
    const intp_t[:] left_counter,
    ndarray[intp_t] right):

    cdef:
        Py_ssize_t i, j, k, count = 0
        ndarray[intp_t] right_sorter
        ndarray[intp_t] right_count
        ndarray[intp_t] right_indexer
        intp_t lc, rc
        Py_ssize_t loc, left_pos = 0, right_pos = 0, position = 0
        Py_ssize_t offset

    # get indexer from existing table
    right_sorter, right_count = groupsort_indexer(right, ngroups)
    with nogil:
        for i in range(1, max_groups + 1):
            rc = right_count[i]
            if rc > 0 and lc > 0:
                count += left_count[i] * rc

    # start joining, build on left
    left_pos = left_count[0]
    right_pos = right_count[0]

    # join result indexer: pos -> pos in indexer
    left_indexer = np.empty(count, dtype=np.intp)
    right_indexer = np.empty(count, dtype=np.intp)

    with nogil:
        for i in range(1, max_groups + 1):
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

# helper functions
# take values from indexer
# TODO: in place?
@cython.wraparound(False)
@cython.boundscheck(False)
def deindex(
    const intp_t[:] values,
    const intp_t[:] indexer,
    intp_t[::1] out
):
    cdef:
        Py_ssize_t i, n, idx
        intp_t fv

    n = indexer.shape[0]

    fv = fill_value

    with nogil:
        for i in range(n):
            idx = indexer[i]
            out[i] = values[idx]

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
    

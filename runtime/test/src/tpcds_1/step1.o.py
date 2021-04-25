#@ type: compute
#@ dependents:
#@   - step3
#@ corunning:
#@   1_out_mem:
#@     trans: 1_out_mem
#@     type: rdma

import pickle
import os
import datetime
import pandas as pd
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
import numpy_groupies as npg
from npjoin import join

scheme_in = {
        "d_date_sk":            np.dtype(np.float32),
        "d_date_id":            np.dtype('S16'),
        "d_date":               np.dtype('S10'),
        "d_month_seq":          np.dtype(np.float32),
        "d_week_seq":           np.dtype(np.float32),
        "d_quarter_seq":        np.dtype(np.float32),
        "d_year":               np.dtype(np.float32),
        "d_dow":                np.dtype(np.float32),
        "d_moy":                np.dtype(np.float32),
        "d_dom":                np.dtype(np.float32),
        "d_qoy":                np.dtype(np.float32),
        "d_fy_year":            np.dtype(np.float32),
        "d_fy_quarter_seq":     np.dtype(np.float32),
        "d_fy_week_seq":        np.dtype(np.float32),
        "d_day_name":           np.dtype('S16'),
        "d_quarter_name":       np.dtype('S8'),
        "d_holiday":            np.dtype('S1'),
        "d_weekend":            np.dtype('S1'),
        "d_following_holiday":  np.dtype('S1'),
        "d_first_dom":          np.dtype(np.float32),
        "d_last_dom":           np.dtype(np.float32),
        "d_same_day_ly":        np.dtype(np.float32),
        "d_same_day_lq":        np.dtype(np.float32),
        "d_current_day":        np.dtype('S1'),
        "d_current_week":       np.dtype('S1'),
        "d_current_month":      np.dtype('S1'),
        "d_current_quarter":    np.dtype('S1'),
        "d_current_year":       np.dtype('S1'),
        }

def build_dtype(schema):
    return np.dtype({
        'names'   : list(schema.keys()),
        'formats' : list(schema.values()),
        })

def main(_, action):
    tag_print = "step1"
    print(f"[tpcds] {tag_print}: begin")

    print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/date_dim.csv"
    csv = urllib.request.urlopen(tableurl)

    df = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))
    print(f"[tpcds] {tag_print}: finish reading csv")
    print('df: ', df.dtype, df.itemsize, df.shape)

    df = df[df['d_year']==2000][['d_date_sk']]
    df = rfn.repack_fields(df)

    # build transport
    trans_name = '1_out_mem'
    print(f"[tpcds] {tag_print}: starting writing back")
    trans = action.get_transport(trans_name, 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)

    # write back
    buffer_pool = buffer_pool_lib.buffer_pool({trans_name:trans})
    print(f"[tpcds] {tag_print}: starting rdma")
    rdma_array = remote_array(buffer_pool, input_ndarray=df, transport_name=trans_name)
    print(f"[tpcds] {tag_print}: finish rdma")

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


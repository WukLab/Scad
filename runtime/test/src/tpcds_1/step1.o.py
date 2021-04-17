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

def main(_, action):
    print("[tpcds] step1: begin")

    print("[tpcds] step1: start reading csv")
    tableurl = "http://localhost:8123/date_dim.csv"
    csv = urllib.request.urlopen(tableurl)

    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")
    print("[tpcds] step1: finish reading csv")

    df = df[df['d_year']==2000][['d_date_sk']]

    # build transport
    print("[tpcds] starting writing back")
    trans = action.get_transport('1_out_mem', 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)

    # write back
    to_write = df.to_numpy()

    buffer_pool = buffer_pool_lib.buffer_pool(trans)
    rdma_array = remote_array(buffer_pool, input_ndarray=to_write)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'columns': list(df.columns),
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


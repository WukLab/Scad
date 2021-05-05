#@ type: compute
#@ dependents:
#@   - mid_1_3
#@ corunning:
#@   1_out_mem:
#@     trans: 1_out_mem
#@     type: rdma

import pickle
import numpy as np
import base64
import urllib.request

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
from npjoin import join
from disaggrt.util import *

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
        'names': list(schema.keys()),
        'formats': list(schema.values()),
    })

def main(_, action):
    # print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/date_dim.csv"
    csv = urllib.request.urlopen(tableurl)

    df = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))
    # print(f"[tpcds] {tag_print}: finish reading csv")
    # print('df: ', df.dtype, df.itemsize, df.shape)

    # data operation
    df = df[df['d_year']==2000]

    # partition the data, and write it to output buffer
    s3_groups = 2
    indexer, groups = join.partition_on_float32(df['d_date_sk'], s3_groups)

    output_size = len(indexer)
    output_dtype = [('d_date_sk', 'f4')]
    output_bytes = output_size * np.dtype(output_dtype).itemsize

    # write output data
    trans_name = '1_out_mem'
    trans = action.get_transport(trans_name, 'rdma')
    trans.reg(output_bytes)

    output_array = asArray(trans, output_dtype, output_size)
    output_array['d_date_sk'] = df[['d_date_sk']][indexer]

    # send our requests
    trans.write(output_bytes, 0)

    # transfer the metedata
    res = {
        # groups, array of groups 
        'groups': [groups.tolist()],
        'size': output_bytes
    }
    return res


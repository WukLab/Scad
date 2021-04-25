#@ type: compute
#@ dependents:
#@   - step8
#@ corunning:
#@   7_out_mem:
#@     trans: 7_out_mem
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
    "s_store_sk": np.dtype(np.float32),
    "s_store_id": np.dtype('S16'),
    "s_rec_start_date": np.dtype('S10'),
    "s_rec_end_date": np.dtype('S10'),
    "s_closed_date_sk": np.dtype(np.float32),
    "s_store_name": np.dtype('S50'),
    "s_number_employees": np.dtype(np.float32),
    "s_floor_space": np.dtype(np.float32),
    "s_hours": np.dtype('S20'),
    "s_manager": np.dtype('S40'),
    "s_market_id": np.dtype(np.float32),
    "s_geography_class": np.dtype('S100'),
    "s_market_desc": np.dtype('S100'),
    "s_market_manager": np.dtype('S40'),
    "s_division_id": np.dtype(np.float32),
    "s_division_name": np.dtype('S50'),
    "s_company_id": np.dtype(np.float32),
    "s_company_name": np.dtype('S50'),
    "s_street_number": np.dtype('S10'),
    "s_street_name": np.dtype('S60'),
    "s_street_type": np.dtype('S15'),
    "s_suite_number": np.dtype('S10'),
    "s_city": np.dtype('S60'),
    "s_county": np.dtype('S30'),
    "s_state": np.dtype('S2'),
    "s_zip": np.dtype('S10'),
    "s_country": np.dtype('S20'),
    "s_gmt_offset": np.dtype(np.float32),
    "s_tax_precentage": np.dtype(np.float32),
}

def build_dtype(schema):
    return np.dtype({
        'names'   : list(schema.keys()),
        'formats' : list(schema.values()),
        })

def main(_, action):
    tag_print = "step7"
    print(f"[tpcds] {tag_print}: begin")

    print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/store.csv"
    csv = urllib.request.urlopen(tableurl)

    df = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))
    df = df[['s_state', 's_store_sk']][df['s_state'] == 'TN'.encode()]
    df = rfn.repack_fields(df, align=True)

    # build transport
    print(f"[tpcds] {tag_print}: starting writing back")
    trans = action.get_transport('7_out_mem', 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)

    # write back
    buffer_pool = buffer_pool_lib.buffer_pool(trans)
    rdma_array = remote_array(buffer_pool, input_ndarray=df)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


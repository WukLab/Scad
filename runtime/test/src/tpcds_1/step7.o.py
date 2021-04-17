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

scheme_in = {
    "s_store_sk": np.dtype(np.float32),
    "s_store_id": np.dtype(16),
    "s_rec_start_date": np.dtype('S10'),
    "s_rec_end_date": np.dtype('S10'),
    "s_closed_date_sk": np.dtype(np.float32),
    "s_store_name": np.dtype(50),
    "s_number_employees": np.dtype(np.float32),
    "s_floor_space": np.dtype(np.float32),
    "s_hours": np.dtype(20),
    "s_manager": np.dtype(40),
    "s_market_id": np.dtype(np.float32),
    "s_geography_class": np.dtype(100),
    "s_market_desc": np.dtype(100),
    "s_market_manager": np.dtype(40),
    "s_division_id": np.dtype(np.float32),
    "s_division_name": np.dtype(50),
    "s_company_id": np.dtype(np.float32),
    "s_company_name": np.dtype(50),
    "s_street_number": np.dtype(10),
    "s_street_name": np.dtype(60),
    "s_street_type": np.dtype(15),
    "s_suite_number": np.dtype(10),
    "s_city": np.dtype(60),
    "s_county": np.dtype(30),
    "s_state": np.dtype(2),
    "s_zip": np.dtype(10),
    "s_country": np.dtype(20),
    "s_gmt_offset": np.dtype(np.float32),
    "s_tax_precentage": np.dtype(np.float32),
}

def main(_, action):
    tag_print = "step7"
    print(f"[tpcds] {tag_print}: begin")

    print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/store.csv"
    csv = urllib.request.urlopen(tableurl)

    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")
    print(f"[tpcds] {tag_print}: finish reading csv")

    df = df[['s_state', 's_store_sk']]
    df = df[df['s_state'] == 'TN']

    # build transport
    print(f"[tpcds] {tag_print}: starting writing back")
    trans = action.get_transport('7_out_mem', 'rdma')
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


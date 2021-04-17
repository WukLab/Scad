#@ type: compute
#@ dependents:
#@   - step6
#@ corunning:
#@   5_out_mem:
#@     trans: 5_out_mem
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
    "c_customer_sk": np.dtype(np.float32),
    "c_customer_id": np.dtype('S16'),
    "c_current_cdemo_sk": np.dtype(np.float32),
    "c_current_hdemo_sk": np.dtype(np.float32),
    "c_current_addr_sk": np.dtype(np.float32),
    "c_first_shipto_date_sk": np.dtype(np.float32),
    "c_first_sales_date_sk": np.dtype(np.float32),
    "c_salutation": np.dtype('S10'),
    "c_first_name": np.dtype('S20'),
    "c_last_name": np.dtype('S30'),
    "c_preferred_cust_flag": np.dtype('S1'),
    "c_birth_day": np.dtype(np.float32),
    "c_birth_month": np.dtype(np.float32),
    "c_birth_year": np.dtype(np.float32),
    "c_birth_country": np.dtype('S20'),
    "c_login": np.dtype('S13'),
    "c_email_address": np.dtype('S50'),
    "c_last_review_date": np.dtype('S10'),
}

def main(_, action):
    tag_print = "step5"
    print(f"[tpcds] {tag_print}: begin")

    print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/customer.csv"
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

    df = df[['c_customer_sk', 'c_customer_id']]

    # build transport
    print(f"[tpcds] {tag_print}: starting writing back")
    trans = action.get_transport('5_out_mem', 'rdma')
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


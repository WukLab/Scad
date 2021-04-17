#@ type: compute
#@ dependents:
#@   - step3
#@ corunning:
#@   2_out_mem:
#@     trans: 2_out_mem
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
        "sr_returned_date_sk":np.dtype(np.float32),
        "sr_return_time_sk":np.dtype(np.float32),
        "sr_item_sk":np.dtype(np.float32),
        "sr_customer_sk":np.dtype(np.float32),
        "sr_cdemo_sk":np.dtype(np.float32),
        "sr_hdemo_sk":np.dtype(np.float32),
        "sr_addr_sk":np.dtype(np.float32),
        "sr_store_sk":np.dtype(np.float32),
        "sr_reason_sk":np.dtype(np.float32),
        "sr_ticket_number":np.dtype(np.float32),
        "sr_return_quantity":np.dtype(np.float32),
        "sr_return_amt":np.dtype(np.float32),
        "sr_return_tax":np.dtype(np.float32),
        "sr_return_amt_inc_tax":np.dtype(np.float32),
        "sr_fee":np.dtype(np.float32),
        "sr_return_ship_cost":np.dtype(np.float32),
        "sr_refunded_cash":np.dtype(np.float32),
        "sr_reversed_charge":np.dtype(np.float32),
        "sr_store_credit":np.dtype(np.float32),
        "sr_net_loss":np.dtype(np.float32)
    }

def main(_, action):
    print("[tpcds] step2: begin")

    print("[tpcds] step2: start reading csv")
    tableurl = "http://localhost:8123/store_returns.csv"
    csv = urllib.request.urlopen(tableurl)

    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")
    print("[tpcds] step2: finish reading csv")

    # Why use this variable here?
    wanted_columns = ['sr_customer_sk',
        'sr_store_sk',
        'sr_return_amt',
        'sr_returned_date_sk']

    # build transport
    print("[tpcds] starting writing back")
    trans = action.get_transport('2_out_mem', 'rdma')
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


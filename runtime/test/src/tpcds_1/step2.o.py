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

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn
import numpy_groupies as npg
from npjoin import join

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

def build_dtype(schema):
    return np.dtype({
        'names'   : list(schema.keys()),
        'formats' : list(schema.values()),
        })

def main(_, action):
    tag_print = "step2"
    print(f"[tpcds] {tag_print}: begin")

    print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/store_returns.csv"
    csv = urllib.request.urlopen(tableurl)

    df = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))
    print(f"[tpcds] {tag_print}: finish reading csv")

    # Why use this variable here?
    #wanted_columns = ['sr_customer_sk',
    #    'sr_store_sk',
    #    'sr_return_amt',
    #    'sr_returned_date_sk']

    # build transport
    trans_name = '2_out_mem'
    print(f"[tpcds] {tag_print}: starting writing back")
    trans = action.get_transport(trans_name, 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)

    # write back
    buffer_pool = buffer_pool_lib.buffer_pool({trans_name:trans})
    rdma_array = remote_array(buffer_pool, input_ndarray=df, transport_name=trans_name)

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


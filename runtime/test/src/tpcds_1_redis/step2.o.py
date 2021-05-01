#@ type: compute
#@ dependents:
#@   - step3

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy import genfromtxt

import pandas as pd
import redis
from io import BytesIO
import datetime

redis_hostname = "localhost"
redis_port = 6379
redis_client = None

def redis_write_pd(key, table, client=redis_client):
    csv_buffer = BytesIO()
    table.to_csv(csv_buffer, sep="|", header=False, index=False)
    if client == None:
        redis_client = redis.Redis(host=redis_hostname, port=redis_port, db=0)
        client = redis_client
    client.set(key, csv_buffer.getvalue())

scheme_in = {
    "sr_returned_date_sk": np.dtype(np.float32),
    "sr_return_time_sk": np.dtype(np.float32),
    "sr_item_sk": np.dtype(np.float32),
    "sr_customer_sk": np.dtype(np.float32),
    "sr_cdemo_sk": np.dtype(np.float32),
    "sr_hdemo_sk": np.dtype(np.float32),
    "sr_addr_sk": np.dtype(np.float32),
    "sr_store_sk": np.dtype(np.float32),
    "sr_reason_sk": np.dtype(np.float32),
    "sr_ticket_number": np.dtype(np.float32),
    "sr_return_quantity": np.dtype(np.float32),
    "sr_return_amt": np.dtype(np.float32),
    "sr_return_tax": np.dtype(np.float32),
    "sr_return_amt_inc_tax": np.dtype(np.float32),
    "sr_fee": np.dtype(np.float32),
    "sr_return_ship_cost": np.dtype(np.float32),
    "sr_refunded_cash": np.dtype(np.float32),
    "sr_reversed_charge": np.dtype(np.float32),
    "sr_store_credit": np.dtype(np.float32),
    "sr_net_loss": np.dtype(np.float32)
}

def main(_, action):
    tag_print = "step2"
    print(f"[tpcds] {tag_print}: begin")

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

    table_key = 'df_step2'
    redis_write_pd(table_key, df)

    context_dict = {}
    context_dict["key"] = table_key
    context_dict["names"] = df.columns
    context_dict["dtypes"] = df.dtypes

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


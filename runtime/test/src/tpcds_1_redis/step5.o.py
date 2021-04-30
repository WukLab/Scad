#@ type: compute
#@ dependents:
#@   - step6

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

def redis_read_pd(key, names, dtypes, client=redis_client):
    if client == None:
        redis_client = redis.Redis(host=redis_hostname, port=redis_port, db=0)
        client = redis_client

    dtypes_raw = dtypes
    if isinstance(dtypes_raw, dict):
        dtypes = dtypes_raw
    else:
        dtypes = {}
        for i in range(len(names)):
            dtypes[names[i]] = dtypes_raw[i]

    parse_dates = []
    for d in dtypes:
        if dtypes[d] == datetime.datetime or dtypes[d] == np.datetime64:
            parse_dates.append(d)
            dtypes[d] = np.dtype("str")
  
    return pd.read_table(BytesIO(client.get(key)), 
                              delimiter="|", 
                              header=None, 
                              names=names,
                              dtype=dtypes, 
                              parse_dates=parse_dates)

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

    df = df[['c_customer_sk', 'c_customer_id']]

    table_key = 'df_step5'
    redis_write_pd(table_key, df)

    context_dict = {}
    context_dict["key"] = table_key
    context_dict["names"] = df.columns
    context_dict["dtypes"] = df.dtypes

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


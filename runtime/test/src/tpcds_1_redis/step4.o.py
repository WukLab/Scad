#@ type: compute
#@ parents:
#@   - step3
#@ dependents:
#@   - step6

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy.lib import recfunctions as rfn
import numpy_groupies as npg

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

def main(params, action):
    tag_print = "step4"
    print(f"[tpcds] {tag_print}: begin")

    context_dict = pickle.loads(base64.b64decode(params['step3']['meta']))
    df_step3 = redis_read_pd(context_dict['key'], context_dict['names'], context_dict['dtypes'])

    df = df_step3.groupby(['sr_customer_sk', 'sr_store_sk']).agg(
        {'sr_return_amt': 'sum'}).reset_index()
    df.rename(columns={'sr_store_sk': 'ctr_store_sk',
                       'sr_customer_sk': 'ctr_customer_sk',
                       'sr_return_amt': 'ctr_total_return'
                       }, inplace=True)
  
    table_key = 'df_step4'
    redis_write_pd(table_key, df)

    context_dict = {}
    context_dict["key"] = table_key
    context_dict["names"] = df.columns
    context_dict["dtypes"] = df.dtypes

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


#@ type: compute
#@ parents:
#@   - step1
#@ dependents:
#@   - step5

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import *
from numpy_groupies.aggregate_numpy import aggregate

from npjoin import join
from numpy.lib import recfunctions as rfn

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
    tag_print = "step2"
    print(f"[tpcds] {tag_print}: begin")

    context_dict = pickle.loads(base64.b64decode(params['step1']['meta']))
    cs = redis_read_pd(context_dict['key'], context_dict['names'], context_dict['dtypes'])

    wh_uc = cs.groupby(['ws_order_number']).agg({'ws_warehouse_sk':'nunique'})
    target_order_numbers = wh_uc.loc[wh_uc['ws_warehouse_sk'] > 1].index.values

    cs_sj_f1 = cs[['ws_order_number']].drop_duplicates()

    df = cs_sj_f1.loc[cs_sj_f1['ws_order_number'].isin(target_order_numbers)]

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


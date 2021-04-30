#@ type: compute
#@ dependents:
#@   - step8

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy import genfromtxt
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

def main(_, action):
    tag_print = "step7"
    print(f"[tpcds] {tag_print}: begin")

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

    df = df[['s_state', 's_store_sk']]
    df = df[df['s_state'] == 'TN'.encode()]

    table_key = 'df_step7'
    redis_write_pd(table_key, df)

    context_dict = {}
    context_dict["key"] = table_key
    context_dict["names"] = df.columns
    context_dict["dtypes"] = df.dtypes

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


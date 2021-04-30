#@ type: compute
#@ dependents:
#@   - step5

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
    "ws_sold_date_sk": np.dtype(np.float32),
    "ws_sold_time_sk": np.dtype(np.float32),
    "ws_ship_date_sk": np.dtype(np.float32),
    "ws_item_sk": np.dtype(np.float32),
    "ws_bill_customer_sk": np.dtype(np.float32),
    "ws_bill_cdemo_sk": np.dtype(np.float32),
    "ws_bill_hdemo_sk": np.dtype(np.float32),
    "ws_bill_addr_sk": np.dtype(np.float32),
    "ws_ship_customer_sk": np.dtype(np.float32),
    "ws_ship_cdemo_sk": np.dtype(np.float32),
    "ws_ship_hdemo_sk": np.dtype(np.float32),
    "ws_ship_addr_sk": np.dtype(np.float32),
    "ws_web_page_sk": np.dtype(np.float32),
    "ws_web_site_sk": np.dtype(np.float32),
    "ws_ship_mode_sk": np.dtype(np.float32),
    "ws_warehouse_sk": np.dtype(np.float32),
    "ws_promo_sk": np.dtype(np.float32),
    "ws_order_number": np.dtype(np.float32),
    "ws_quantity": np.dtype(np.float32),
    "ws_wholesale_cost": np.dtype(np.float32),
    "ws_list_price": np.dtype(np.float32),
    "ws_sales_price": np.dtype(np.float32),
    "ws_ext_discount_amt": np.dtype(np.float32),
    "ws_ext_sales_price": np.dtype(np.float32),
    "ws_ext_wholesale_cost": np.dtype(np.float32),
    "ws_ext_list_price": np.dtype(np.float32),
    "ws_ext_tax": np.dtype(np.float32),
    "ws_coupon_amt": np.dtype(np.float32),
    "ws_ext_ship_cost": np.dtype(np.float32),
    "ws_net_paid": np.dtype(np.float32),
    "ws_net_paid_inc_tax": np.dtype(np.float32),
    "ws_net_paid_inc_ship": np.dtype(np.float32),
    "ws_net_paid_inc_ship_tax": np.dtype(np.float32),
    "ws_net_profit": np.dtype(np.float32),
}

def main(_, action):
    tag_print = "step3"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/web_sales.csv"
    csv = urllib.request.urlopen(tableurl)

    names = list(scheme_in.keys()) + ['']
    df = pd.read_table(csv, 
            delimiter="|", 
            header=None, 
            names=names,
            usecols=range(len(names)-1), 
            dtype=scheme_in,
            na_values = "-")

    wanted_columns = ['ws_order_number',
                      'ws_ext_ship_cost',
                      'ws_net_profit',
                      'ws_ship_date_sk',
                      'ws_ship_addr_sk',
                      'ws_web_site_sk',
                      'ws_warehouse_sk']

    df = df[wanted_columns]

    table_key = 'df_step3'
    redis_write_pd(table_key, df)

    context_dict = {}
    context_dict["key"] = table_key
    context_dict["names"] = df.columns
    context_dict["dtypes"] = df.dtypes

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


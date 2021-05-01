#@ type: compute
#@ dependents:
#@   - step2
#@ corunning:
#@   1_out_mem:
#@     trans: 1_out_mem
#@     type: rdma

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy import genfromtxt
from numpy.lib import recfunctions as rfn

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

def build_dtype(schema):
    return np.dtype({
        'names': list(schema.keys()),
        'formats': list(schema.values()),
    })

def main(_, action):
    tag_print = "step1"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/web_sales.csv"
    csv = urllib.request.urlopen(tableurl)

    df = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))
    # print(f"[tpcds] {tag_print}: finish reading csv")
    # print('df: ', df.dtype, df.itemsize, df.shape)

    # data operation
    wanted_columns = ['ws_order_number',
                      'ws_warehouse_sk']
    df = df[wanted_columns]
    df = rfn.repack_fields(df)

    # build transport
    trans_name = '1_out_mem'
    # print(f"[tpcds] {tag_print}: start writing back")
    trans = action.get_transport(trans_name, 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)

    # write back
    buffer_pool = buffer_pool_lib.buffer_pool({trans_name:trans})
    rdma_array = remote_array(buffer_pool, input_ndarray=df, transport_name=trans_name)
    # print(f"[tpcds] {tag_print}: finish writing back")

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


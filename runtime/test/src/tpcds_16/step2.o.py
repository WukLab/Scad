#@ type: compute
#@ dependents:
#@   - step3
#@ corunning:
#@   2_out_mem:
#@     trans: 2_out_mem
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
    "cr_returned_date_sk": np.dtype(np.float32),
    "cr_returned_time_sk": np.dtype(np.float32),
    "cr_item_sk": np.dtype(np.float32),
    "cr_refunded_customer_sk": np.dtype(np.float32),
    "cr_refunded_cdemo_sk": np.dtype(np.float32),
    "cr_refunded_hdemo_sk": np.dtype(np.float32),
    "cr_refunded_addr_sk": np.dtype(np.float32),
    "cr_returning_customer_sk": np.dtype(np.float32),
    "cr_returning_cdemo_sk": np.dtype(np.float32),
    "cr_returning_hdemo_sk": np.dtype(np.float32),
    "cr_returning_addr_sk": np.dtype(np.float32),
    "cr_call_center_sk": np.dtype(np.float32),
    "cr_catalog_page_sk": np.dtype(np.float32),
    "cr_ship_mode_sk": np.dtype(np.float32),
    "cr_warehouse_sk": np.dtype(np.float32),
    "cr_reason_sk": np.dtype(np.float32),
    "cr_order_number": np.dtype(np.float32),
    "cr_return_quantity": np.dtype(np.float32),
    "cr_return_amount": np.dtype(np.float32),
    "cr_return_tax": np.dtype(np.float32),
    "cr_return_amt_inc_tax": np.dtype(np.float32),
    "cr_fee": np.dtype(np.float32),
    "cr_return_ship_cost": np.dtype(np.float32),
    "cr_refunded_cash": np.dtype(np.float32),
    "cr_reversed_charge": np.dtype(np.float32),
    "cr_store_credit": np.dtype(np.float32),
    "cr_net_loss": np.dtype(np.float32),
}

def build_dtype(schema):
    return np.dtype({
        'names': list(schema.keys()),
        'formats': list(schema.values()),
    })

def main(_, action):
    tag_print = "step2"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/catalog_returns.csv"
    csv = urllib.request.urlopen(tableurl)

    df = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))
    # print(f"[tpcds] {tag_print}: finish reading csv")

    # data operation
    df = df['cr_order_number']
    df = rfn.repack_fields(df)

    # build transport
    trans_name = '2_out_mem'
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


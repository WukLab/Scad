#@ type: compute
#@ dependents:
#@   - step5
#@ corunning:
#@   4_out_mem:
#@     trans: 4_out_mem
#@     type: rdma

import pickle
import numpy as np
import base64
import urllib.request
import disaggrt.buffer_pool_lib as buffer_pool_lib
from disaggrt.rdma_array import remote_array

from numpy import genfromtxt

# import pandas as pd

scheme_in = {
  "ca_address_sk": np.dtype(np.float32) ,
  "ca_address_id": np.dtype('S16') ,
  "ca_street_number": np.dtype('S10') ,
  "ca_street_name": np.dtype('S60') ,
  "ca_street_type": np.dtype('S15') ,
  "ca_suite_number": np.dtype('S10') ,
  "ca_city": np.dtype('S60') ,
  "ca_county": np.dtype('S30') ,
  "ca_state": np.dtype('S2') ,
  "ca_zip": np.dtype('S10') ,
  "ca_country": np.dtype('S20') ,
  "ca_gmt_offset": np.dtype(np.float32) ,
  "ca_location_type": np.dtype('S20') ,
}

def build_dtype(schema):
    return np.dtype({
        'names': list(schema.keys()),
        'formats': list(schema.values()),
    })

def main(_, action):
    tag_print = "step4"
    print(f"[tpcds] {tag_print}: begin")

    # print(f"[tpcds] {tag_print}: start reading csv")
    tableurl = "http://localhost:8123/customer_address.csv"
    csv = urllib.request.urlopen(tableurl)

    cs = genfromtxt(csv, delimiter='|', dtype=build_dtype(scheme_in))
    # print(f"[tpcds] {tag_print}: finish reading csv")

    # data operation
    cs = cs[cs['ca_state'] == 'GA'.encode()][['ca_address_sk']]
    print(f"[tpcds] {tag_print} df_step4: ", cs.dtype, cs.shape)

    # build transport
    trans_name = '4_out_mem'
    # print(f"[tpcds] {tag_print}: start writing back")
    trans = action.get_transport(trans_name, 'rdma')
    trans.reg(buffer_pool_lib.buffer_size)

    # write back
    buffer_pool = buffer_pool_lib.buffer_pool({trans_name:trans})
    rdma_array = remote_array(buffer_pool, input_ndarray=cs, transport_name=trans_name)
    # print(f"[tpcds] {tag_print}: finish writing back")

    # debug
    # df = pd.DataFrame(data=cs, columns=['ca_address_sk'])
    # df.to_csv('/home/jil/serverless/Disagg-Serverless/runtime/test/src/tpcds_16/step4.csv')

    # transfer the metedata
    context_dict = {}
    context_dict["df"] = rdma_array.get_array_metadata()
    context_dict["bp"] = buffer_pool.get_buffer_metadata()

    context_dict_in_byte = pickle.dumps(context_dict)
    return {
        'meta': base64.b64encode(context_dict_in_byte).decode('ascii')
    }


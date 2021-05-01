from tpcds_1_include import *

# depends on 3
def main(args, action):
  ret_dict = args['step3']
  d = read_table_rdma(action, ret_dict['io_dict'])

  df = d.groupby(['sr_customer_sk', 'sr_store_sk']).agg(
      {'sr_return_amt': 'sum'}).reset_index()
  df.rename(columns={'sr_store_sk': 'ctr_store_sk',
                     'sr_customer_sk': 'ctr_customer_sk',
                     'sr_return_amt': 'ctr_total_return'
                     }, inplace=True)

  transport_name = "output_step4"
  io_dict = write_table_rdma(action, transport_name, df)

  return {'io_dict': io_dict}

action = buffer_pool_lib.action_setup()
context_dict = buffer_pool_lib.read_params()
main(context_dict, action)

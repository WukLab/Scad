from tpcds_1_include import *

# depends on 4 and 5
def main(args, action):
  ret_dict = args['step4']
  df_1 = read_table_rdma(action, ret_dict['io_dict'])
  ret_dict = args['step5']
  df_2 = read_table_rdma(action, ret_dict['io_dict'])

  df = df_1.merge(df_2, left_on='ctr_customer_sk', right_on='c_customer_sk')
  df = df[['c_customer_id','ctr_store_sk','ctr_customer_sk','ctr_total_return']]

  transport_name = "output_step6"
  io_dict = write_table_rdma(action, transport_name, df)

  return {'io_dict': io_dict}

action = buffer_pool_lib.action_setup()
context_dict = buffer_pool_lib.read_params()
main(context_dict, action)
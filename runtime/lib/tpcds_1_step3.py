from tpcds_1_include import *

# depends on 1 and 2
def main(args, action):
  ret_dict = args['step1']
  df_1 = read_table_rdma(action, ret_dict['io_dict'])
  ret_dict = args['step2']
  df_2 = read_table_rdma(action, ret_dict['io_dict'])

  df = df_1.merge(df_2, left_on='d_date_sk', right_on='sr_returned_date_sk')
  df.drop('d_date_sk', axis=1, inplace=True)

  transport_name = "output_step3"
  io_dict = write_table_rdma(action, transport_name, df)

  return {'io_dict': io_dict}

action = buffer_pool_lib.action_setup()
context_dict = buffer_pool_lib.read_params()
main(context_dict, action)
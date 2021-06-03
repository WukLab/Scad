#!/usr/bin/env python3
import pandas
from matplotlib import pyplot as plt
from matplotlib.lines import Line2D
import itertools
import json

MARKERS = itertools.cycle(Line2D.filled_markers)

def plot_fig2():
  df = pandas.read_csv('./fig2/data.csv')
  df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
  systems = ['Porus', 'OpenWhisk', 'AWS Step Functions']
  lengths = [2, 8]
  fig = plt.figure()
  for system in systems:
    for length in lengths:
      tmp = df[df["sys"] == system]
      tmp = tmp[tmp['parallelism'] == length]
      tmp = tmp.iloc[:,[0,2,3,4,-1]]
      percent_overhead = 100 * (tmp.iloc[:,-1] / tmp.iloc[:,3])
      # aaa = df.query('sys == "{}" and chain_length == "{}"'.format(system, length))
      # aaa = df['chain_length' == length]
      # print(percent_overhead)
      plt.plot(tmp.iloc[:,2], percent_overhead, label='{} {} func fan out'.format(system, length), marker=next(MARKERS))
      # y_data = df[:-1]


  axes = plt.gca()
  # plt.rcParams.update({'font.size': 16})
  axes.set_xscale('log')
  plt.grid()
  plt.title('Execuction Time Percentage Overhead in Function Fan-Out', fontsize=20)
  plt.xlabel('Single Function Runtime (ms)', fontsize=18)
  plt.ylabel('Percentage of Runtime as Scheduling Overhead', fontsize=18)
  plt.legend()
  plt.show()
  plt.savefig('tmp.png')

def plot_fig1():

  df = pandas.read_csv('./fig1/data.csv')
  df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
  print(df)
  systems = ['Porus', 'OpenWhisk', 'AWS Step Functions']
  lengths = [2, 8]
  fig = plt.figure()
  # print(df)
  for system in systems:
    for length in lengths:
      tmp = df[df["sys"] == system]
      tmp = tmp[tmp['chain_length'] == length]
      tmp = tmp.iloc[:,[0,2,3,4,-1]]
      percent_overhead = 100 * (tmp.iloc[:,-1] / tmp.iloc[:,3])
      # aaa = df.query('sys == "{}" and chain_length == "{}"'.format(system, length))
      # aaa = df['chain_length' == length]
      # print(percent_overhead)
      plt.plot(tmp.iloc[:,2], percent_overhead, label='{} {} func sequence'.format(system, length), marker=next(MARKERS))
      # y_data = df[:-1]


  axes = plt.gca()
  # plt.rcParams.update({'font.size': 16})
  axes.set_xscale('log')
  plt.grid()
  plt.title('Execuction Time Percentage Overhead in Function Sequences', fontsize=20)
  plt.xlabel('Single Function Runtime (ms)', fontsize=18)
  plt.ylabel('Percentage of Runtime as Scheduling Overhead', fontsize=18)
  plt.legend()
  plt.show()
  plt.savefig('tmp.png')

  # print(df[:,-1])


def plot_fig3():
  raw_data = None
  with open('./exp3/data2.json') as f:
    raw_data = json.load(f)
  tputs = pandas.DataFrame([], columns=["concurrent", "op_type", "tput"])
  lats = pandas.DataFrame([], columns=["concurrent", "op_type", "size", "lat"])
  for x in raw_data:
    concurrent_elems = raw_data[x]
    for measurement_type in concurrent_elems:
      measurement = concurrent_elems[measurement_type]
      if measurement_type == "tput":
        for mes in measurement:
          for op_type in mes:
            row = {'concurrent':int(x), 'op_type':op_type, 'tput':mes[op_type]}
            tputs = tputs.append([row], ignore_index=True)
      elif measurement_type == 'lat':
        for exp in measurement:
          for size in exp:
            mes = exp[size]
            # print(mes)
            for op_type in mes:
              row = { 'concurrent': int(x), 'op_type':op_type, 'size':size, 'lat': mes[op_type]}
              lats = lats.append(row, ignore_index=True)
  tputs.to_csv('./exp3/tputs.csv')
  lats.to_csv('./exp3/lats.csv')
  # redis parsing
  with open('./redis.json') as f:
    raw_data = json.load(f)
  tputs = pandas.DataFrame([], columns=["concurrent", "op_type", "tput"])
  lats = pandas.DataFrame([], columns=["concurrent", "op_type", "size", "lat"])
  for x in raw_data:
    concurrent_elems = raw_data[x]
    for data_point in concurrent_elems:
      for measurement_type in data_point:
        measurement = data_point[measurement_type]
        if measurement_type == "tput":
            for op_type in measurement:
              row = {'concurrent':int(x), 'op_type':op_type, 'tput':measurement[op_type]}
              tputs = tputs.append([row], ignore_index=True)
        elif measurement_type == 'lats':
            for size in measurement:
              mes = measurement[size]
              for op_type in mes:
                row = { 'concurrent': int(x), 'op_type':op_type, 'size':size, 'lat': mes[op_type]}
                lats = lats.append(row, ignore_index=True)
  tputs.to_csv('./exp3/redis-tputs.csv')
  lats.to_csv('./exp3/redis-lats.csv')




FIGURES = {
  # 'fig1': plot_fig1,
  # 'fig2': plot_fig2,
  'exp3': plot_fig3
}


def main():
  for figure in FIGURES:
    FIGURES[figure]()

if __name__ == "__main__":
  main()
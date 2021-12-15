#!/usr/bin/env python3

import argparse
import re
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.patches as mpatches

import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

def parse_raw_resources(file):
  res = []
  for line in open(file, 'r'):
    temp = re.findall(r'\d+', line)
    nums = list(map(int, temp))
    [2021, 12, 14, 9, 25, 1, 514, 71, 0, 95104, 183808, 7, 0, 175616, 175616, 48, 0, 49152, 196608]
    tm = datetime.datetime(nums[0], nums[1], nums[2], nums[3], nums[4], nums[5], 1000*nums[6])
    line = [
      tm,
      nums[7],
      nums[9],
      nums[10],
      nums[11],
      nums[13],
      nums[14],
      nums[15],
      nums[17],
      nums[18]
    ]
    res.append(line)
  return res

def main():

  parser = argparse.ArgumentParser()
  parser.add_argument("--file", help='file to parse', default="resources.txt")

  args = parser.parse_args()

  result = parse_raw_resources(args.file)
  cols = [
    'time',
    'cpu-cpu',
    'cpu-mem',
    'cpu-storage',
    'mem-cpu',
    'mem-mem',
    'mem-storage',
    'bal-cpu',
    'bal-mem',
    'bal-storage',
  ]
  df = pd.DataFrame(result, columns=cols)
  print(df)

#   fig, ax = plt.subplots(3, 1)
  fig, ax = plt.subplots(1, 1)
  ax = [ax]


  timestart = df[['time']].min()
  df[['time']] -= timestart

  start = 350
  df['time'] = df['time'].dt.total_seconds() - start
#   end = df['time'].max()
  end = 236
  df = df[df['time']>0]
  df = df[df['time']<236]
#   print(end)
  print(df)
#   df['time'] = df[df['time'] < end]
#   print(df[df['time'] < end])
  df['bal-mem'] = df['bal-mem'] / 1024
  df['cpu-mem'] = 100 - df['cpu-mem'] / 1024
  df['mem-mem'] = 100 - (df['mem-mem'] / df['mem-mem'].max() * 100)

  df['bal-mem'] = 100 - (df['bal-mem'] / df['bal-mem'].max() * 100)
  df['bal-cpu'] = 100 - (df['bal-cpu'] / df['bal-cpu'].max() * 100)
  df['cpu-cpu'] = 100 - (df['cpu-cpu'] / df['cpu-cpu'].max() * 100)


  # plot CPU
  df.plot(x='time', y='bal-cpu', color='blue', ax=ax[0])
#   df.plot(x='time', y='cpu-cpu', color='blue', ax=ax[1])

  # plot mem
  color = 'tab:red'
  axs = [x.twinx() for x in ax]
  df.plot(x='time', y='bal-mem', color=color, ax=ax[0])
#   df.plot(x='time', y='cpu-mem', color=color, ax=ax[1])
#   df.plot(x='time', y='mem-mem', color=color, ax=ax[2])

#   ax[0].set_xticklabels([])
#   ax[1].set_xticklabels([])
  #by mohammad
  for axx in ax:
      axx.yaxis.set_major_formatter(mtick.PercentFormatter())
      axx.get_legend().remove()
      axx.set_ylim(0, 100)
  for axx in axs:
      axx.set_yticklabels([])
      if axx.get_legend() is not None:
          axx.get_legend().remove()

  ax[0].hlines(70, 0, end, linestyles='dashed')
  ax[0].text(19, 52, 'High Util.\nWatermark', horizontalalignment='center')
  
  ax[0].text(87.5, 105, 'Increasing App. 1 Load', horizontalalignment='center')
  ax[0].annotate(text='',xytext=(25, 100), xycoords='data',
				xy=(150, 100),textcoords='data',
				arrowprops=dict(arrowstyle="|-|",lw=4),va='center')

  ax[0].text(190, 105, 'Decreasing App. 1 Load', horizontalalignment='center')
  ax[0].annotate(text='',xytext=(236, 100), xycoords='data',
				xy=(148, 100),textcoords='data',
				arrowprops=dict(arrowstyle="|-|",lw=4),va='center')
  
  ax[0].vlines(63, 0, 100, linestyles='dotted')
  ax[0].text(65, 7, 'App. 2\nSplit', horizontalalignment='left')

  ax[0].vlines(117, 0, 100, linestyles='dotted')
  ax[0].text(119, 7, 'App. 3\nSplit', horizontalalignment='left')

  ax[0].vlines(200, 0, 100, linestyles='dotted')
  ax[0].text(199, 12, 'App. 3\nMerged', horizontalalignment='right')

  ax[0].vlines(223, 0, 100, linestyles='dotted')
  ax[0].text(222, 35, 'App. 2\nMerged', horizontalalignment='right')

#   ax[0].set_xlabel("")
#   ax[1].set_xlabel("")
#   ax[2].set_xlabel("")
  ax[0].set_xlim(0,end)
  # ax[1].set_xlim(0,300)

  plt.subplots_adjust(hspace = 0.3)
#   ax[0].set_title('Balanced Pool')
#   ax[1].set_title('Compute Pool')
#   ax[2].set_title('Memory Pool')


  ax[0].set_xlabel('Time (s)', fontsize=15)
  fig.text(0.01, 0.5, 'Resource Utilization', va='center', rotation='vertical', fontsize=15)

  red_patch = mpatches.Patch(color='red', label='Memory')
  blue_patch = mpatches.Patch(color='blue', label='CPU')
  plt.legend(handles=[blue_patch, red_patch], loc='upper right')
  fig.set_size_inches(6.5, 2.8)
  plt.subplots_adjust(bottom=0.2, right=0.95)
#   handles, labels = ax[0].get_legend_handles_labels()
#   ax[0].legend(handles[::-1], labels[::-1], )
  plt.savefig('figure-policy.pdf')



if __name__ == "__main__":
  main()

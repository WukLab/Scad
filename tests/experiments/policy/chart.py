#!/usr/bin/env python3

import argparse
import re
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.patches as mpatches

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

  fig, ax = plt.subplots(1, 1)
  ax = [ax]


  timestart = df[['time']].min()
  df[['time']] -= timestart

  start = 350
  df['time'] = df['time'].dt.total_seconds() - start
  end = df['time'].max()
  df['time'] = df[df['time'] < end]
  df['bal-mem'] = df['bal-mem'] / 1024
  df['cpu-mem'] = df['cpu-mem'] / 1024
  df['mem-mem'] = 100 - (df['mem-mem'] / df['mem-mem'].max() * 100)

  df['bal-mem'] = 100 - (df['bal-mem'] / df['bal-mem'].max() * 100)
  df['bal-cpu'] = 100 - (df['bal-cpu'] / df['bal-cpu'].max() * 100)
  df['cpu-cpu'] = 100 - (df['cpu-cpu'] / df['cpu-cpu'].max() * 100)


  # plot CPU
  df.plot(x='time', y='bal-cpu', color='blue', ax=ax[0])
  # df.plot(x='time', y='cpu-cpu', color='blue', ax=ax[1])

  # plot mem
  color = 'tab:red'
  axs = [x.twinx() for x in ax]
  df.plot(x='time', y='bal-mem', color=color, ax=axs[0])
  # df.plot(x='time', y='cpu-mem', color=color, ax=axs[1])
  # df.plot(x='time', y='mem-mem', color=color, ax=ax[2])

  ax[0].set_xticklabels([])
  # ax[1].set_xticklabels([])
  for axx in ax:
      axx.yaxis.set_major_formatter(mtick.PercentFormatter())
      axx.get_legend().remove()
  for axx in axs:
      axx.set_yticklabels([])
      if axx.get_legend() is not None:
          axx.get_legend().remove()

  ax[0].set_xlabel("")
  # ax[1].set_xlabel("")
  ax[0].set_xlim(0,end)
  # ax[1].set_xlim(0,300)

  plt.subplots_adjust(hspace = 0.3)
  ax[0].set_title('Balanced Pool')
  # ax[1].set_title('Compute Pool')

  ax[0].set_xlabel('Execution Time (s)', fontsize=16)
  fig.text(0.01, 0.5, 'Resource Utilization', va='center', rotation='vertical', fontsize=16)

  red_patch = mpatches.Patch(color='red', label='Memory')
  blue_patch = mpatches.Patch(color='blue', label='CPU')
  plt.legend(handles=[red_patch, blue_patch], loc='lower left')

  plt.savefig('figure-policy.pdf')



if __name__ == "__main__":
  main()

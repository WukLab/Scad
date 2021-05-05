from disagg import *
import os
import numpy as np

# 2M request size
requestSize = 1024 * 1024 * 2

# helper function for loading
def remoteRead(trans, offset, raddr, size):
    print('read size: {}, local: {}, remote: {}'.format(
        size, offset, raddr))
    for i in range(0, size, requestSize):
        s = min(requestSize, size - i)
        trans.read(s, raddr + i, offset + i)

def remoteWrite(trans, offset, raddr, size):
    print('write size: {}, local: {}, remote: {}'.format(
        size, offset, raddr))
    for i in range(0, size, requestSize):
        s = min(requestSize, size - i)
        trans.write(s, raddr + i, offset + i)

# require par > 1
def initTrans(action, name, par, bufsize):
    t0name = name + '@0' if par > 1 else name
    t0 = action.get_transport(t0name, 'rdma')
    t0.reg(bufsize)
    trans = [t0]
    if par > 1:
        for i in range(1,par):
            t = action.get_transport(name+'@'+str(i), 'rdma')
            t.reg(bufsize, t0)
            trans.append(t)
    return trans

class ArrayLoader:
    def __init__(self, trans, dtype,
        groups, groupOffsets):

        self.cur = 0
        self.dtype = dtype
        self.itemsize = dtype.itemsize
        self.trans = trans

        # init arr for view
        self.index = 0
        self.offset = 0
        self.numGroups = len(groups)
        self.view = np.asarray(trans[0].buf).view(dtype = dtype)

        self.groups = groups
        self.groupOffsets = groupOffsets

    def getAll(self, offset = 0):
        n = sum(self.groups)
        return self.getNext(n, offset)

    def getNext(self, n, offset = 0):
        rem = n
        while (rem > 0):
            curTrans = self.trans[self.cur]
            rrem = self.groups[self.cur]
            roffset = self.groupOffsets[self.cur]
            if rem >= rrem:
                size = rrem * self.itemsize
                remoteRead(curTrans, offset,
                    roffset * self.itemsize, size)
                rem -= rrem
                offset += size
                self.cur += 1
                if self.cur == self.numGroups:
                    break
            else:
                size = rem * self.itemsize
                remoteRead(curTrans, offset,
                    roffset * self.itemsize, size)
                self.groupsOffset[self.cur] += rem
                self.groups[self.cur] -= rem
                offset += size
                rem = 0
                break
        # todo: return the array slice
        start = self.index
        self.index += n
        return self.view[start:self.index]

def asArray(trans, dtype, size):
    return np.asarray(trans.buf).view(dtype)[:size]

def getParIndex():
    return int(os.environ['__OW_PAR'])

def getGroups(arr, trans, i):
    groups = []
    groupOffset = []
    groupTrans = []
    for t in range(len(arr)):
        a = arr[t]
        _groups  = [g[i + 1] - g[i] for g in a['groups']] 
        _offsets = [g[i] for g in a['groups']] 
        _trans = [trans[t] for _ in range(len(_groups))]
        groups.extend(_groups)
        groupOffset.extend(_offsets)
        groupTrans.extend(_trans)
    return groups, groupOffset, groupTrans

tpcds1Par = [0, 1, 1, 1, 1, 1, 1, 1, 1]

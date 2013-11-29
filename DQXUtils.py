import time
import os

class Timer:
    def __init__(self):
        self.t0=time.time()
        self.t1=time.clock()
    def Elapsed(self):
        return time.time()-self.t0
    def ElapsedCPU(self):
        return time.clock()-self.t1


def GetDQXServerPath():
    return os.path.dirname(os.path.realpath(__file__))
import time

class Timer:
    def __init__(self):
        self.t0=time.clock()
    def Elapsed(self):
        return time.clock()-self.t0

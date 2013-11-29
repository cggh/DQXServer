import time
import os
import re

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


identifierMatcher = re.compile(r"^[^\d\W]\w*\Z")

def CheckValidIdentifier(id):
    if re.match(identifierMatcher, id) is None:
        raise Exception('Invalid identifier: '+id)

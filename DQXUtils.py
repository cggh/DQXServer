# This file is part of DQXServer - (C) Copyright 2014, Paul Vauterin, Ben Jeffery, Alistair Miles <info@cggh.org>
# This program is free software licensed under the GNU Affero General Public License.
# You can find a copy of this license in LICENSE in the top directory of the source code or at <http://opensource.org/licenses/AGPL-3.0>

import time
import os
import re


def LogServer(line):
    print('@@@'+line)


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

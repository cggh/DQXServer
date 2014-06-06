# This file is part of DQXServer - (C) Copyright 2014, Paul Vauterin, Ben Jeffery, Alistair Miles <info@cggh.org>
# This program is free software licensed under the GNU Affero General Public License.
# You can find a copy of this license in LICENSE in the top directory of the source code or at <http://opensource.org/licenses/AGPL-3.0>

import sys
import importlib


class Wrapper(object):
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __getattr__(self, name):
        if name == '__file__':
            raise AttributeError

        module = importlib.import_module('respondersraw.' + name)
        return module

sys.modules[__name__+'raw'] = sys.modules[__name__]
sys.modules[__name__] = Wrapper(sys.modules[__name__])


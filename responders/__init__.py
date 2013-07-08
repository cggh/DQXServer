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


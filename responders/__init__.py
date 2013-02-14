import sys
import importlib


class Wrapper(object):
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __getattr__(self, name):
        try:
            module = importlib.import_module('respondersraw.' + name)
        except ImportError:
            raise AttributeError
        return module.response

sys.modules[__name__+'raw'] = sys.modules[__name__]
sys.modules[__name__] = Wrapper(sys.modules[__name__])


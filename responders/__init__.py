import sys
import pageqry


class Wrapper(object):
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __getattr__(self, name):
        try:
            module = __import__(name, globals(), locals(), [], 1)
        except ImportError:
            raise AttributeError
        return module.response


sys.modules[__name__] = Wrapper(sys.modules[__name__])


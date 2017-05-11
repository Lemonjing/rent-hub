import functools

def new_fn(f):
    @functools.wraps(f)
    def fn(x):
        print 'call'+f.__name__+'()'
        return f(x)
    return fn

@new_fn
def f1(x):
    return x*2

print f1.__name__


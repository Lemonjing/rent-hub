import functools
import math
import datetime
import os
import time
import sys
import requests
import Config


def new_fn(f):
    @functools.wraps(f)
    def fn(x):
        print 'call' + f.__name__ + '()'
        return f(x)

    return fn


@new_fn
def f1(x):
    return x * 2


print f1.__name__

a = math.sqrt(8)
b = int(a)
print a
print b


# Closure

def count():
    fs = []
    for i in range(1, 4):
        def f():
            return i * i

        fs.append(f)
    return fs


f1, f2, f3 = count()

list1 = [1, 2, 3]
a, b, c = list1

print a
print b
print c

d = list1
print d

str1 = '2012-1-4'
str2 = '22:00:55'

print datetime.datetime.strptime(str1, '%Y-%m-%d')

date_today = datetime.date.today()
date = datetime.datetime.strptime(str2, '%H:%M:%S')
print date
date = date.replace(year=date_today.year, month=date_today.month, day=date_today.day)
print date

a = os.path.split(os.path.realpath('/Users/saber/GitHub/a.txt'))[0]
b = os.path.split(os.path.realpath('/Users/saber/GitHub/a.txt'))[1]
print a
print b
print os.path.join(a, b)

a = time.localtime()
FILETIMEFORMAT = '%Y%m%d_%X'
print time.strftime(FILETIMEFORMAT, a).replace(":", "")


url = 'https://www.douban.com'
r = requests.get(url)
this_file_dir = os.path.split(os.path.realpath(__file__))[0]
config_file_path = os.path.join(this_file_dir, 'config.ini')
config = Config.Config(config_file_path)
config.update(r.cookies)
print config.douban_cookie

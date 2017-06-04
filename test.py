# -*- coding: utf-8 -*-

import functools
import math
import datetime
import os
import time
import sys
import requests
import Config
import sqlite3
from bs4 import BeautifulSoup
import re

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


def process():
    # result_file_name = 'results/result_' + str(sqltime)
    # creat database
    dict_topics = {}
    try:
        conn = sqlite3.connect('results/result_renthub.sqlite')
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute('select * from rent order by posttime DESC limit 1')
        posttime = cursor.fetchall()[0][6]
        print posttime
        print type(posttime)
    except Exception, e:
        print 'database error'
    cursor.close()
    conn.commit()
    conn.close()

# process()

url = 'https://www.douban.com/group/topic/102929560/'
r = requests.get(url)
if r.status_code == 200:
    soup = BeautifulSoup(r.text, 'html.parser')
    content_soup = soup.find_all(attrs={'class': 'topic-content'})[1]
    if content_soup.find_all('img'):
        cover_image = content_soup.find_all('img')[0].get('src')
        print cover_image


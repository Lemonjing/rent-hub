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

url = 'https://www.douban.com/group/146409/discussion?start=0'
r = requests.get(url)
if r.status_code == 200:
    soup = BeautifulSoup(r.text, 'html.parser')
    table = soup.find_all(attrs={'class': 'olt'})[0]
    paginator = soup.find_all(attrs={'class': 'paginator'})[0]
    filter_count = 0
    for tr in table.find_all('tr'):
        if filter_count <= 2:
            filter_count += 1
            continue
        td = tr.find_all('td')
        print td[0].string
        print td[0].text
        print '========='
        title_text = td[0].find_all('a')[0].get('title')
        link_text = td[0].find_all('a')[0].get('href')
        user_text = td[1].a.string
        reply_count = td[2].string
        time_text = td[3].string
        break
print title_text
print link_text
print user_text
print reply_count
print time_text


def getTimeFromStr(timeStr):
    # 日期时间都转成datetime
    datetime_today = datetime.datetime.today()
    if '-' in timeStr and ':' in timeStr:
        # 字符串包含日期和时间
        prefix_str = timeStr.split(' ')[0]
        suffix_str = timeStr.split(' ')[1]
        if len(prefix_str) <= 5 and len(suffix_str) == 8:
            # 如 1-1 11:00:00 or 01-01 11:00:00
            print 'Detect No Year of datetime-str.',
            dt = datetime.datetime.strptime(timeStr, "%m-%d %H:%M:%S")
            return dt.replace(year=datetime_today.year)
        elif len(prefix_str) <= 5 and len(suffix_str) < 8:
            # 如 1-1 11:00 or 01-01 11:00
            print 'Detect No Year & No Second of datetime-str.',
            dt = datetime.datetime.strptime(timeStr, "%m-%d %H:%M")
            return dt.replace(year=datetime_today.year)
        else:
            # 如 2017-01-01 11:00:00
            print 'Detect correct datetime-str.',
            return datetime.datetime.strptime(timeStr, "%Y-%m-%d %H:%M:%S")
    elif '-' in timeStr:
        # 字符串仅有日期
        if len(timeStr) <= 5:
            print 'Detect No Year of date-str.',
            dt = datetime.datetime.strptime(timeStr, "%m-%d")
            return dt.replace(year=datetime_today.year)
        else:
            print 'Detect correct date-str.',
            return datetime.datetime.strptime(timeStr, "%Y-%m-%d")
    elif ':' in timeStr:
        # 字符串仅有时间
        if len(timeStr) < 8:
            print 'Detect No Second of time-str.',
            dt = datetime.datetime.strptime(timeStr, "%H:%M")
        else:
            print 'Detect correct time-str.',
            dt = datetime.datetime.strptime(timeStr, "%H:%M:%S")
        # date.replace(year, month, day)：生成一个新的日期对象
        return dt.replace(year=datetime_today.year, month=datetime_today.month, day=datetime_today.day)
    else:
        # 返回当前的datetime对象
        return datetime_today


str1 = raw_input("enter:")
print str1


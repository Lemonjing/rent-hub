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
        conn = sqlite3.connect('results/result_20170521_204824.sqlite')
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute('SELECT id, url FROM rent ORDER BY id ASC')
        values = cursor.fetchall()
        print values
        for v in values:
            dict_topics[v[0]] = v[1]
        print dict_topics
    except Exception, e:
        print 'database error'
        return
    finally:
        cursor.close()

    douban_headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,en-GB;q=0.2,zh-TW;q=0.2',
        'Connection': 'keep-alive',
        'DNT': '1',
        'HOST': 'www.douban.com',
        'Cookie': ''
    }

    cursor = conn.cursor()

    keys_topics = dict_topics.keys()
    for k in keys_topics:
        if k == 5:
            break
        r = requests.get(dict_topics[k], headers=douban_headers)
        if r.status_code == 200:
            try:
                douban_headers['Cookie'] = r.cookies
                soup = BeautifulSoup(r.text, 'html.parser')
                userface_soup = soup.find_all(attrs={'class': 'user-face'})[0]
                print 'hello'
                head_image = userface_soup.find_all('img')[0].get('src')
                user_name = userface_soup.find_all('img')[0].get('alt')
                content_soup = soup.find_all(attrs={'class': 'topic-content'})[1]
                content = unicode(content_soup)
                print user_name, head_image, content
                try:
                    cursor.execute('UPDATE rent SET user=?, headimage=?, content=? WHERE id=?', \
                                   [user_name, head_image, content, k])
                except Exception, e:
                    print 'update database error', e
            except Exception, e:
                print 'error match soup:', e
        time.sleep(5)
    cursor.close()
    conn.commit()
    conn.close()


process()

# tstr = '<div111><p>本人在这里住了好几年，因工作原因现转租。<br/>房价1025，这几年就涨了25块。所以就这个价。<br/>出租的是单间，房间里空调电视机都有。还有一张很大的书桌，见下图。<br/>联系电话17621066365。<br/>小区名称：海东公寓，到地铁站一公里，如果到陆家嘴门口公交可以直达。</p>'
# pattern1 = re.compile('.*<p>.*</p>.*')
# matchornot = pattern1.match(tstr)
# if matchornot:
#     res = matchornot.group()
#     target1 = res[res.index('<p>'):res.index('</p>')]
#     target2 = target1.replace("<br/>", "\\n")
#     print target2

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

url = 'https://www.douban.com/group/topic/102732276/'
r = requests.get(url)
if r.status_code == 200:
    try:
        soup = BeautifulSoup(r.text, 'html.parser')
        post_time = soup.find_all(attrs={'class': 'color-green'})[0].string
        userface_soup = soup.find_all(attrs={'class': 'user-face'})[0]
        userface_soup_img = userface_soup.find_all('img')[0]
        head_image = userface_soup_img.get('src')
        user_name = userface_soup_img.get('alt')
        content_soup = soup.find_all(attrs={'class': 'topic-content'})[1]
        print '=====content_soup over====='
        print '=====cover_image start====='
        if content_soup.find_all('img'):
            cover_image = content_soup.find_all('img')[0].get('src')
        else:
            cover_image = None
        print '=====cover_image end====='
        content = str(content_soup)
        print '=======detail data end======'
        # print user_name, head_image, content

    except Exception, e:
        print 'error match soup:', e.message
        print 'error url', url
        print '正在过滤错误url，请稍后...'

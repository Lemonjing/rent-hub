#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import sys
import time
import datetime
import os
import requests
from bs4 import BeautifulSoup

import Config


class Utils(object):
    @staticmethod
    def isInBalckList(blacklist, text):
        if blacklist:
            return False
        for item in blacklist:
            if text.find(item) != -1:
                return True
        return False

    @staticmethod
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


class Crawler(object):
    douban_black_list = '搬家'

    def __init__(self, config):
        self.config = config
        self.douban_headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,en-GB;q=0.2,zh-TW;q=0.2',
            'Connection': 'keep-alive',
            'DNT': '1',
            'HOST': 'www.douban.com',
            'Cookie': self.config.douban_cookie
        }

    def run(self):
        result_file_name = 'results/result_renthub'
        try:
            print '打开数据库...'
            # creat database
            conn = sqlite3.connect(result_file_name + '.sqlite')
            conn.text_factory = str
            cursor = conn.cursor()
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS rent(id INTEGER PRIMARY KEY, user TEXT, headimage TEXT, title TEXT, content TEXT, \
url TEXT UNIQUE, posttime timestamp, crawtime timestamp, source TEXT, note TEXT)')
            cursor.close()
            cursor = conn.cursor()

            key_word_list = self.config.key_word_list  # 爬取的关键字列表
            custom_black_list = self.config.custom_black_list  # 自定义关键字黑名单列表
            start_time = Utils.getTimeFromStr(self.config.start_time)  # 爬取的开始时间

            '''
            爬虫初始url集合
            入参：页面编号
            '''

            def urlList(page_number):
                num_in_url = str(page_number * 50)

                '''
                douban_url = [
                    'https://www.douban.com/group/search?start=' + num_in_url + '&group=146409&cat=1013&sort=time&q=',
                    'https://www.douban.com/group/search?start=' + num_in_url + '&group=523355&cat=1013&sort=time&q=',
                    'https://www.douban.com/group/search?start=' + num_in_url + '&group=557646&cat=1013&sort=time&q=',
                    'https://www.douban.com/group/search?start=' + num_in_url + '&group=383972&cat=1013&sort=time&q=',
                    'https://www.douban.com/group/search?start=' + num_in_url + '&group=283855&cat=1013&sort=time&q=',
                    'https://www.douban.com/group/search?start=' + num_in_url + '&group=76231&cat=1013&sort=time&q=',
                    'https://www.douban.com/group/search?start=' + num_in_url + '&group=196844&cat=1013&sort=time&q=',
                    'https://www.douban.com/group/search?start=' + num_in_url + '&group=259227&cat=1013&sort=time&q=']
                '''
                douban_url = [
                    'https://www.douban.com/group/146409/discussion?start=' + num_in_url]

                return douban_url

            '''
            与初始url一一对应的群组名
            '''
            douban_url_name = ['上海租房', '上海招聘，租房', '上海租房(2)', '上海合租族_魔都租房', '上海租房@浦东租房', \
                               '上海租房---房子是租来的，生活不是', '上海租房@长宁租房/徐汇/静安租房', '上海租房（不良中介勿扰）']

            def crawl(index, currentUrl, douban_headers):
                url_link = currentUrl
                print 'url_link: ', url_link
                r = requests.get(url_link, headers=douban_headers)
                if r.status_code == 200:
                    try:
                        if index == 0:
                            self.douban_headers['Cookie'] = r.cookies
                            print self.douban_headers['Cookie']
                        soup = BeautifulSoup(r.text, 'html.parser')
                        paginator = soup.find_all(attrs={'class': 'paginator'})[0]
                        # print "=========paginator: =", paginator

                        if (page_number != 0) and not paginator:
                            return False
                        else:
                            try:
                                table = soup.find_all(attrs={'class': 'olt'})[0]
                                tr_count_for_this_page = 0
                                filter_count = 0
                                for tr in table.find_all('tr'):
                                    if filter_count <= 2:
                                        filter_count += 1
                                        continue
                                    td = tr.find_all('td')
                                    title_text = td[0].find_all('a')[0].get('title')
                                    # ignore items in blacklist
                                    if Utils.isInBalckList(custom_black_list, title_text):
                                        continue
                                    if Utils.isInBalckList(self.douban_black_list, title_text):
                                        continue
                                    link_text = td[0].find_all('a')[0].get('href')
                                    user_text = td[1].a.string
                                    reply_count = td[2].string
                                    update_time_text = td[3].string

                                    # ignore data ahead of the specific date
                                    if Utils.getTimeFromStr(update_time_text) < start_time:
                                        boot.ok = False
                                        return False
                                    tr_count_for_this_page += 1

                                    try:
                                        cursor.execute(
                                            'INSERT INTO rent(id, user, headimage, title, content, url, posttime, updatetime, crawtime, \
                                              source, note) VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                            [user_text, None, title_text, None, link_text, None,
                                             Utils.getTimeFromStr(update_time_text),
                                             datetime.datetime.now(), douban_url_name[index], reply_count])
                                        print 'add new data:', title_text, link_text, update_time_text, \
                                            datetime.datetime.now(), douban_url_name[index], reply_count
                                    except sqlite3.Error, e:
                                        print 'data exists:', link_text, e  # 之前添加过了而URL（设置了唯一）一样会报错
                            except Exception, e:
                                print 'error match table:', e
                    except Exception, e:
                        print 'error match paginator:', e
                        boot.ok = False
                        return False
                else:
                    print 'request url error %s -status code: %s:' % (url_link, r.status_code)
                time.sleep(self.config.douban_sleep_time)

            print '爬虫开始运行...'

            douban_url = urlList(0)
            for i in range(len(douban_url)):
                page_number = 0

                # i is url index
                print 'start url[%d]:' % i
                boot.ok = True
                page_number = 0
                print '>>>>>>>>>> Search %s ...' % douban_url_name[i]

                while boot.ok:
                    print 'i, page_number: ', i, page_number

                    currentUrl = urlList(page_number)[i]
                    crawl(i, currentUrl, self.douban_headers)
                    page_number += 1
        except Exception, e:
            print 'crawl run() error:', e.message
        cursor.close()
        conn.commit()
        conn.close()
        print '爬虫运行结束。sqlite文件生成。'
        print '========================================='
        print '''
            ##########     ##########
            #        #     #        #
            #        #     #        #
            #   ##   #     #   ##   #
            #        #     #        #
            #        #     #        #
            ##########     ##########

            房子是租来的，但生活不是。
            '''
        print '========================================='

    '''
    1.准备详情页的数据（用户、正文数据）
    2.过滤无效数据，写入参数（如不存在的用户发文信息）
        参数total_count start_time
    3.写入脚本运行数据到db_hub.sqlite
        update_time（更新时间）, total_count（总信息数）, update_count（更新信息数）
    '''

    def process(self):
        print '================='
        print '现在开始对数据表进行处理'
        print '处理1.准备详情页的数据...'
        # 1
        dict_topics = {}
        try:
            conn = sqlite3.connect('results/result_renthub.sqlite')
            conn.text_factory = str
            cursor = conn.cursor()

            print '========DEBUG1========'
            print self.config.total_count

            cursor.execute('SELECT id, url FROM rent WHERE id >=? ORDER BY id ASC', [self.config.max_id])
            values = cursor.fetchall()
            print values
            for v in values:
                dict_topics[v[0]] = v[1]
        except Exception, e:
            print 'database error', e
        finally:
            cursor.close()

        cursor = conn.cursor()
        print '========DEBUG2========'
        keys_topics = dict_topics.keys()
        for k in keys_topics:
            r = requests.get(dict_topics[k], headers=self.douban_headers)
            if r.status_code == 200:
                try:
                    self.douban_headers['Cookie'] = r.cookies
                    soup = BeautifulSoup(r.text, 'html.parser')
                    post_time = soup.find_all(attrs={'class': 'color-green'})[0].string
                    userface_soup = soup.find_all(attrs={'class': 'user-face'})[0]
                    head_image = userface_soup.find_all('img')[0].get('src')
                    user_name = userface_soup.find_all('img')[0].get('alt')
                    content_soup = soup.find_all(attrs={'class': 'topic-content'})[1]
                    content = str(content_soup)
                    # print user_name, head_image, content
                    try:
                        cursor.execute('UPDATE rent SET user=?, headimage=?, content=?, posttime=? WHERE id=?', \
                                       [user_name, head_image, content, post_time, k])
                    except Exception, e:
                        print 'update database error', e
                except Exception, e:
                    print 'error match soup:', e.message
                    print 'error url', dict_topics[k]
                    continue
            time.sleep(self.config.douban_sleep_time)
        cursor.close()
        print '处理1完成。'

        # 2
        print '处理2.过滤无效数据，写入配置文件...'
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM rent WHERE user IS NULL ')
            total_count = len(cursor.execute('SELECT * FROM rent').fetchall())
            print '处理2 total_count', total_count

            cursor.execute('SELECT * FROM rent ORDER BY posttime DESC LIMIT 1')
            cursor.execute('SELECT * FROM rent ORDER BY id DESC LIMIT 1')
            max_id = cursor.fetchall()[0][0]
            post_time = cursor.fetchall()[0][6]

            try:
                self.config.update('db', 'total_count', total_count)
                self.config.update('db', 'max_id', max_id)
                self.config.update('common', 'start_time', post_time)
            except Exception, e:
                print 'write total_count, start_time to config.ini error.', e

            cursor.close()
        except Exception, e:
            print 'delete null data error', e
        conn.commit()
        conn.close()
        print '处理2完成。'

        # 3
        print '处理3.写入脚本运行数据...'
        try:
            conn_db = sqlite3.connect("results/db_hub.sqlite")
            conn_db.text_factory = str
            cursor_db = conn_db.cursor()
            cursor_db.execute(
                'CREATE TABLE IF NOT EXISTS db_hub(id INTEGER PRIMARY KEY, update_time timestamp, \
    total_count TEXT, update_count TEXT)')
            res_count = len(cursor_db.execute('SELECT * FROM db_hub').fetchall())
            print 'res_count', res_count

            if res_count == 0:
                cursor_db.execute('INSERT INTO db_hub(id, update_time, total_count, update_count) VALUES (NULL, \
                    ?, ?, ?)', [datetime.datetime.now(), total_count, total_count])
            else:
                cursor_db.execute('SELECT total_count FROM db_hub WHERE id = ?', [res_count])
                pre_total_count = int(cursor_db.fetchone()[0])
                cursor_db.execute('INSERT INTO db_hub(id, update_time, total_count, update_count) VALUES (NULL, \
                                                     ?, ?, ?)',
                                  [datetime.datetime.now(), total_count, total_count - pre_total_count])
        except Exception, e:
            print 'database error', e
        cursor_db.close()
        conn_db.commit()
        conn_db.close()
        print '处理3完成。'


class BootDriver(object):
    def __init__(self):
        this_file_dir = os.path.split(os.path.realpath(__file__))[0]
        config_file_path = os.path.join(this_file_dir, 'config.ini')
        self.ok = True
        self.config = Config.Config(config_file_path)
        FILETIMEFORMAT = '%Y%m%d_%X'
        self.file_time = time.strftime(FILETIMEFORMAT, time.localtime()).replace(':', '')
        results_path = os.path.join(sys.path[0], 'results')
        if not os.path.isdir(results_path):
            os.makedirs(results_path)

    def run(self):
        crawler = Crawler(self.config)
        crawler.run()
        crawler.process()


if __name__ == '__main__':
    boot = BootDriver()
    boot.run()

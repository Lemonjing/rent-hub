#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import sys
import time
import datetime
import os
import requests
import multiprocessing
from multiprocessing import Lock
from bs4 import BeautifulSoup

import Config


class Utils(object):
    @staticmethod
    def is_in_balcklist(blacklist, text):
        if blacklist:
            return False
        for item in blacklist:
            if text.find(item) != -1:
                return True
        return False

    @staticmethod
    def datetime_from_str(time_str):
        # 日期时间都转成datetime
        datetime_today = datetime.datetime.today()
        if '-' in time_str and ':' in time_str:
            # 字符串包含日期和时间
            prefix_str = time_str.split(' ')[0]
            suffix_str = time_str.split(' ')[1]
            if len(prefix_str) <= 5 and len(suffix_str) == 8:
                # 如 1-1 11:00:00 or 01-01 11:00:00
                # 'Detect No Year of datetime-str.',
                dt = datetime.datetime.strptime(time_str, "%m-%d %H:%M:%S")
                return dt.replace(year=datetime_today.year)
            elif len(prefix_str) <= 5 and len(suffix_str) < 8:
                # 如 1-1 11:00 or 01-01 11:00
                # 'Detect No Year & No Second of datetime-str.',
                dt = datetime.datetime.strptime(time_str, "%m-%d %H:%M")
                return dt.replace(year=datetime_today.year)
            else:
                # 如 2017-01-01 11:00:00
                # 'Detect correct datetime-str.',
                return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        elif '-' in time_str:
            # 字符串仅有日期
            if len(time_str) <= 5:
                # 'Detect No Year of date-str.',
                dt = datetime.datetime.strptime(time_str, "%m-%d")
                return dt.replace(year=datetime_today.year)
            else:
                # 'Detect correct date-str.',
                return datetime.datetime.strptime(time_str, "%Y-%m-%d")
        elif ':' in time_str:
            # 字符串仅有时间
            if len(time_str) < 8:
                # 'Detect No Second of time-str.',
                dt = datetime.datetime.strptime(time_str, "%H:%M")
            else:
                # 'Detect correct time-str.',
                dt = datetime.datetime.strptime(time_str, "%H:%M:%S")
            # date.replace(year, month, day)：生成一个新的日期对象
            return dt.replace(year=datetime_today.year, month=datetime_today.month, day=datetime_today.day)
        else:
            # 返回当前的datetime对象
            return datetime_today


class Crawler(object):
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
url TEXT UNIQUE, posttime timestamp, updatetime timestamp, crawtime timestamp, source TEXT, note INTEGER, city INTEGER, coverimage TEXT)')
            cursor.close()
            cursor = conn.cursor()

            custom_black_list = self.config.custom_black_list  # 自定义关键字黑名单列表
            start_time = Utils.datetime_from_str(self.config.start_time)  # 爬取的开始时间

            '''
            爬虫初始url集合
            入参：页面编号
            '''

            def urlList(page_number_arg, city):
                # 当前25条分页，若是搜索界面为50条分页
                num_in_url = str(page_number_arg * 25)

                douban_url_sh = [
                    # 上海租房 146409
                    'https://www.douban.com/group/146409/discussion?start=' + num_in_url,
                    # 上海招聘，租房 523355
                    'https://www.douban.com/group/523355/discussion?start=' + num_in_url,
                    # 上海租房(2) 557646
                    'https://www.douban.com/group/557646/discussion?start=' + num_in_url,
                    # 上海合租族_魔都租房 38397r2
                    'https://www.douban.com/group/383972/discussion?start=' + num_in_url,
                    # 上海租房@浦东租房 283855
                    'https://www.douban.com/group/283855/discussion?start=' + num_in_url,
                    # 上海租房---房子是租来的，生活不是 76231
                    'https://www.douban.com/group/76231/discussion?start=' + num_in_url,
                    # 上海租房@长宁租房/徐汇/静安租房 196844
                    'https://www.douban.com/group/196844/discussion?start=' + num_in_url,
                    # 上海租房（不良中介勿扰）259227
                    'https://www.douban.com/group/259227/discussion?start=' + num_in_url]

                douban_url_hz = [
                    # 杭州租房 281316
                    'https://www.douban.com/group/281316/discussion?start=' + num_in_url,
                    # 杭州租房（出租、求租、合租）467221
                    'https://www.douban.com/group/467221/discussion?start=' + num_in_url,
                    # 杭州 出租 租房 中介免入 145219
                    'https://www.douban.com/group/145219/discussion?start=' + num_in_url,
                    # 杭州租房一族 276209
                    'https://www.douban.com/group/276209/discussion?start=' + num_in_url,
                    # 共享天堂---我要租房（杭州）120199
                    'https://www.douban.com/group/120199/discussion?start=' + num_in_url,
                    # 杭州西湖区租房 560075
                    'https://www.douban.com/group/560075/discussion?start=' + num_in_url,
                    # 杭州租房 340633
                    'https://www.douban.com/group/340633/discussion?start=' + num_in_url,
                    # 杭州滨江租房 554566
                    'https://www.douban.com/group/554566/discussion?start=' + num_in_url,
                    # 滨江租房 550725
                    'https://www.douban.com/group/550725/discussion?start=' + num_in_url,
                    # 滨江租房 539160
                    'https://www.douban.com/group/539160/discussion?start=' + num_in_url,
                    # 我要在杭州租房子 224803
                    'https://www.douban.com/group/340633/discussion?start=' + num_in_url,
                    # 杭州无中介租房 562889
                    'https://www.douban.com/group/562889/discussion?start=' + num_in_url]

                # 杭州:0 上海:1 扩展
                if city == 0:
                    douban_url = douban_url_hz
                else:
                    douban_url = douban_url_sh

                return douban_url

            '''
            与初始url一一对应的群组名
            '''
            douban_url_name_hz = ['杭州租房', '杭州租房（出租、求租、合租）', '杭州 出租 租房 中介免入', '杭州租房一族', '共享天堂---我要租房（杭州）', \
                                  '杭州西湖区租房', '杭州租房', '杭州滨江租房', '滨江租房', '滨江租房', '我要在杭州租房子', '杭州无中介租房']

            '''
            与初始url一一对应的群组名
            '''
            douban_url_name_sh = ['上海租房', '上海招聘，租房', '上海租房(2)', '上海合租族_魔都租房', '上海租房@浦东租房', \
                                  '上海租房---房子是租来的，生活不是', '上海租房@长宁租房/徐汇/静安租房', '上海租房（不良中介勿扰）']

            def crawl(index, current_url, douban_headers, city):
                url_link = current_url
                print 'url_link: ', url_link
                r = requests.get(url_link, headers=douban_headers)
                if r.status_code == 200:
                    try:
                        if index == 0:
                            self.douban_headers['Cookie'] = r.cookies
                            # print self.douban_headers['Cookie']
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
                                    if Utils.is_in_balcklist(custom_black_list, title_text):
                                        continue

                                    link_text = td[0].find_all('a')[0].get('href')
                                    user_text = td[1].a.string
                                    reply_count = td[2].string
                                    update_time_text = td[3].string

                                    # ignore data ahead of the specific date
                                    if Utils.datetime_from_str(update_time_text) < start_time:
                                        boot.ok = False
                                        return False
                                    tr_count_for_this_page += 1

                                    try:
                                        cursor.execute(
                                            'INSERT INTO rent(id, user, headimage, title, content, url, posttime, updatetime, crawtime, \
                                              source, note, city) VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                            [user_text, None, title_text, None, link_text, None,
                                             Utils.datetime_from_str(update_time_text),
                                             datetime.datetime.now(), douban_url_name[index], reply_count, city])
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

            for city in range(2):
                if city == 0:
                    douban_url_name = douban_url_name_hz
                else:
                    douban_url_name = douban_url_name_sh
                search_url = urlList(0, city)
                for i in range(len(search_url)):
                    page_number = 0

                    # i is url index
                    print 'start url[%d]:' % i
                    boot.ok = True
                    page_number = 0
                    print '>>>>>>>>>> Search %s ...' % douban_url_name[i]

                    while boot.ok:
                        print 'i, page_number: ', i, page_number

                        current_url = urlList(page_number, city)[i]
                        crawl(i, current_url, self.douban_headers, city)
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
        print '现在开始对数据表进行处理'
        # 1
        print '处理1.准备详情页的数据...'
        # 准备待处理url
        dict_topics = {}
        try:
            conn = sqlite3.connect('results/result_renthub.sqlite')
            conn.text_factory = str
            cursor = conn.cursor()
            cursor.execute('SELECT id, url FROM rent WHERE id>=? ORDER BY id ASC', [self.config.max_id])
            values = cursor.fetchall()
            print values
            for v in values:
                dict_topics[v[0]] = v[1]
        except Exception, e:
            print 'database error', e
        finally:
            cursor.close()

        # 拆分为4进程处理4段url
        keys_topics = dict_topics.keys()
        keys_topics_len = len(keys_topics)
        print 'keys_topics_len=', keys_topics_len
        keys_topics_gap = keys_topics_len / 4
        keys_topics1 = keys_topics[0:keys_topics_gap]
        keys_topics2 = keys_topics[keys_topics_gap:2 * keys_topics_gap]
        keys_topics3 = keys_topics[2 * keys_topics_gap:3 * keys_topics_gap]
        keys_topics4 = keys_topics[3 * keys_topics_gap:]

        print 'keys_topics=', keys_topics
        print 'keys_topics1=', keys_topics1
        print 'keys_topics2=', keys_topics2
        print 'keys_topics3=', keys_topics3
        print 'keys_topics4=', keys_topics4

        # 每一段单独进程
        def parallel(dict_topics_split, f):
            print 'Current Process start --------> '
            for k in dict_topics_split:
                r = requests.get(dict_topics[k], headers=self.douban_headers)
                if r.status_code == 200:
                    try:
                        self.douban_headers['Cookie'] = r.cookies
                        soup = BeautifulSoup(r.text, 'html.parser')
                        post_time = soup.find_all(attrs={'class': 'color-green'})[0].string
                        userface_soup = soup.find_all(attrs={'class': 'user-face'})[0]
                        userface_soup_img = userface_soup.find_all('img')[0]
                        head_image = userface_soup_img.get('src')
                        user_name = userface_soup_img.get('alt')
                        content_soup = soup.find_all(attrs={'class': 'topic-content'})[1]
                        if content_soup.find_all('img'):
                            cover_image = content_soup.find_all('img')[0].get('src')
                        else:
                            cover_image = ''
                        content = unicode(content_soup)
                        # print user_name, head_image, content
                        try:
                            line = user_name.encode("utf-8") + 'delimited' + head_image.encode("utf-8") + 'delimited' \
                                   + content.encode('utf-8') + 'delimited' + post_time.encode('utf-8') \
                                   + 'delimited' + cover_image.encode("utf-8") + 'delimited' + str(k) + 'linesplit'
                            with open(f, "a+") as fs:
                                fs.write(line)
                            # p_cursor.execute(
                            #     'UPDATE rent SET user=?, headimage=?, content=?, posttime=?, coverimage=? WHERE id=?', \
                            #     [user_name, head_image, content, post_time, cover_image, k])
                        except Exception, e:
                            print 'save text line error', e.message
                            print 'current error text line', line
                    except Exception, e:
                        print 'error match soup:', e.message
                        print 'error url', dict_topics[k]
                        print '正在过滤错误url，请稍后...'
                        continue
                time.sleep(self.config.douban_sleep_time)
            print '--------> Current Process end'
        '''
        多进程进行处理1
        '''
        files = ['tmp_file1', 'tmp_file2', 'tmp_file3', 'tmp_file4']
        p1 = multiprocessing.Process(target=parallel, args=(keys_topics1, files[0]))
        p2 = multiprocessing.Process(target=parallel, args=(keys_topics2, files[1]))
        p3 = multiprocessing.Process(target=parallel, args=(keys_topics3, files[2]))
        p4 = multiprocessing.Process(target=parallel, args=(keys_topics4, files[3]))
        p_arr = [p1, p2, p3, p4]
        p1.start()
        p2.start()
        p3.start()
        p4.start()
        print("The number of CPU is:" + str(multiprocessing.cpu_count()))

        # 等待多进程全部运行完毕
        for p in p_arr:
            p.join()
        print "多进程结束，现在开始处理文本..."

        # 处理4进程产生的4个文本
        for filename in files:
            with open(filename, 'r') as fs1:
                lines_tmp = fs1.read().split('linesplit')
                lines = lines_tmp[:-1]
                for line in lines:
                    fields = line.split('delimited')
                    user_name = fields[0]
                    head_image = fields[1]
                    content = fields[2]
                    post_time = fields[3]
                    cover_image = fields[4]
                    k = int(fields[5])
                    try:
                        cursor = conn.cursor()
                        cursor.execute(
                            'UPDATE rent SET user=?, headimage=?, content=?, posttime=?, coverimage=? WHERE id=?', \
                            [user_name, head_image, content, post_time, cover_image, k])
                    except Exception, e:
                        print 'update database error', e.message
                        print 'current error url key', k
                    continue
            try:
                this_file_dir = os.path.split(os.path.realpath(__file__))[0]
                tmp_file_path = os.path.join(this_file_dir, filename)
                if os.path.exists(tmp_file_path):
                    os.remove(tmp_file_path)
            except Exception, e:
                print 'delete file error', e.message
        cursor.close()
        print '处理1完成。'

        # 2
        print '处理2.过滤无效数据，写入配置文件...'
        try:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM rent WHERE user IS NULL OR headimage IS NULL')
            total_count = len(cursor.execute('SELECT * FROM rent').fetchall())
            print '处理null后的最终total_count', total_count

            cursor.execute('SELECT * FROM rent ORDER BY posttime DESC LIMIT 1')
            post_time = cursor.fetchall()[0][6]
            cursor.execute('SELECT * FROM rent ORDER BY id DESC LIMIT 1')
            max_id = cursor.fetchall()[0][0]

            try:
                print '处理2结束时的max_id', max_id
                print '处理2结束时的start_time', post_time
                self.config.update('db', 'max_id', max_id)
                self.config.update('common', 'start_time', post_time)
            except Exception, e:
                print 'write max_id, start_time to config.ini error.', e

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
    total_count INTEGER, update_count INTEGER)')
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
    time1 = time.clock()
    boot = BootDriver()
    boot.run()
    time2 = time.clock()
    print '程序总运行时间为: %f 秒' % (time2 - time1)

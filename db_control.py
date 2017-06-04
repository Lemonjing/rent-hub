#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import time
import Config
import datetime


def process():
    print '处理3.写入脚本运行数据 ...'
    conn = sqlite3.connect('results/result_renthub.sqlite')
    config = Config.Config('config.ini')

    cursor = conn.cursor()

    print 'total_count', total_count
    cursor.close()
    conn.close()

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


process()

# -*- coding:utf-8 -*-
# renthub restful api

from flask import Flask
from flask import request
from flask import jsonify
from flask import send_file
import sqlite3
import sys
import time

'''
==========  ===============================================  =============================
HTTP 方法   URL                                              动作
==========  ===============================================  ==============================
GET         http://[hostname]/todo/api/v1.0/tasks            检索任务列表
GET         http://[hostname]/todo/api/v1.0/tasks/[task_id]  检索某个任务
POST        http://[hostname]/todo/api/v1.0/tasks            创建新任务
PUT         http://[hostname]/todo/api/v1.0/tasks/[task_id]  更新任务
DELETE      http://[hostname]/todo/api/v1.0/tasks/[task_id]  删除任务
==========  ================================================ =============================
'''

app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]


@app.route('/')
def hello_world():
    return 'RentHub Restful API V1.0.'


@app.route('/9437DC9F2456E68DFB3DE48A17FFACE0.txt/')
def ssl_verify():
    return send_file('9437DC9F2456E68DFB3DE48A17FFACE0.txt')


@app.route('/todo/api/v1.0/tasks', methods=['GET'])
def get_tasks():
    return jsonify({'tasks': tasks})


@app.route('/api/sys/', methods=['GET'])
def get_sys():
    info = {}
    try:
        conn = sqlite3.connect('./results/db_hub.sqlite')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM db_hub ORDER BY id DESC LIMIT 1')
        value = cursor.fetchone()
        info = {
            'update_time': value[1],
            'total_count': value[2],
            'update_count': value[3]
        }
    except Exception, e:
        print 'database error', e
    finally:
        cursor.close()
        conn.close()

    return jsonify({'info': info})


@app.route('/api/recommended/', methods=['GET'])
def rmd_list():
    limit = request.args.get('limit')
    rmd_list = []
    try:
        conn = sqlite3.connect('./results/result_renthub.sqlite')
        cursor = conn.cursor()
        cursor.execute(
            'SELECT id, user, headimage, title, updatetime, coverimage, note FROM rent WHERE coverimage IS NOT NULL ORDER BY note DESC, updatetime DESC LIMIT ?',
            [limit])
        values = cursor.fetchall()
        for row in values:
            d = {'id': row[0],
                 'user': row[1],
                 'headimage': row[2],
                 'title': row[3],
                 'updatetime': row[4],
                 'coverimage': row[5]
                 }
            rmd_list.append(d)
    except Exception, e:
        print 'database error', e
    finally:
        cursor.close()
        conn.close()

    return jsonify({'rmd_list': rmd_list})


@app.route('/api/search/all/', methods=['GET'])
def search_all():
    offset = request.args.get('offset')
    limit = request.args.get('limit')
    sort_arg = request.args.get('sort')
    keyword = request.args.get('keyword').replace(' ', '').encode('utf-8')

    topic_list = []
    total_count = 0

    if sort_arg == '1':
        sort = 'posttime'
    elif sort_arg == '2':
        sort = 'note'
    else:
        sort = 'updatetime'

    try:
        conn = sqlite3.connect('./results/result_renthub.sqlite')
        cursor = conn.cursor()
        sql = "SELECT id, user, headimage, title, updatetime, coverimage FROM rent WHERE title LIKE '%" + keyword \
              + "%' ORDER BY " + sort + " DESC LIMIT ?,?"
        print sql
        cursor.execute(sql, [offset, limit])
        values = cursor.fetchall()
        cursor.close()
        for row in values:
            d = {'id': row[0],
                 'user': row[1],
                 'headimage': row[2],
                 'title': row[3],
                 'updatetime': row[4],
                 'coverimage': row[5]
                 }
            topic_list.append(d)
        cursor = conn.cursor()
        sql = "SELECT * FROM rent WHERE title LIKE '%" + keyword + "%'"
        cursor.execute(sql)
        values = cursor.fetchall()
        total_count = len(values)
    except Exception, e:
        print 'database error', e
    finally:
        cursor.close()
        conn.close()

    return jsonify({'topic_list': topic_list, 'total_count': total_count})


@app.route('/api/search/', methods=['GET'])
def search():
    offset = request.args.get('offset')
    limit = request.args.get('limit')
    city = request.args.get('city')
    sort_arg = request.args.get('sort')
    keyword = request.args.get('keyword').replace(' ', '').encode('utf-8')

    topic_list = []
    total_count = 0

    if sort_arg == '1':
        sort = 'posttime'
    elif sort_arg == '2':
        sort = 'note'
    else:
        sort = 'updatetime'

    try:
        conn = sqlite3.connect('./results/result_renthub.sqlite')
        cursor = conn.cursor()
        sql = "SELECT id, user, headimage, title, updatetime, coverimage FROM rent WHERE city=? and title LIKE '%" \
              + keyword + "%' ORDER BY " + sort + " DESC LIMIT ?,?"
        cursor.execute(sql, [city, offset, limit])
        values = cursor.fetchall()
        cursor.close()
        for row in values:
            d = {'id': row[0],
                 'user': row[1],
                 'headimage': row[2],
                 'title': row[3],
                 'updatetime': row[4],
                 'coverimage': row[5]
                 }
            topic_list.append(d)
        cursor = conn.cursor()
        sql = "SELECT * FROM rent WHERE city=? and title LIKE '%" + keyword + "%'"
        cursor.execute(sql, [city])
        values = cursor.fetchall()
        total_count = len(values)
    except Exception, e:
        print 'database error', e
    finally:
        cursor.close()
        conn.close()

    return jsonify({'topic_list': topic_list, 'total_count': total_count})


@app.route('/api/lists/all/', methods=['GET'])
def list_all():
    topic_list = []
    offset = request.args.get('offset')
    limit = request.args.get('limit')
    sort_arg = request.args.get('sort')
    if sort_arg == '1':
        sort = 'posttime'
    elif sort_arg == '2':
        sort = 'note'
    else:
        sort = 'updatetime'
    try:
        conn = sqlite3.connect('./results/result_renthub.sqlite')
        cursor = conn.cursor()
        sql = 'SELECT id, user, headimage, title, updatetime, coverimage FROM rent ORDER BY ' + sort + ' DESC LIMIT ?,?'
        cursor.execute(sql, [offset, limit])
        values = cursor.fetchall()
        for row in values:
            d = {'id': row[0],
                 'user': row[1],
                 'headimage': row[2],
                 'title': row[3],
                 'updatetime': row[4],
                 'coverimage': row[5]
                 }
            topic_list.append(d)
    except Exception, e:
        print 'database error', e
    finally:
        cursor.close()
        conn.close()

    return jsonify({'topic_list': topic_list})


@app.route('/api/lists/', methods=['GET'])
def list():
    offset = request.args.get('offset')
    limit = request.args.get('limit')
    city = request.args.get('city')
    sort_arg = request.args.get('sort')
    topic_list = []

    if sort_arg == '1':
        sort = 'posttime'
    elif sort_arg == '2':
        sort = 'note'
    else:
        sort = 'updatetime'

    try:
        conn = sqlite3.connect('./results/result_renthub.sqlite')
        cursor = conn.cursor()
        sql = "SELECT id, user, headimage, title, updatetime, coverimage FROM rent WHERE city= ? ORDER BY " + sort + " DESC LIMIT ?, ?"
        cursor.execute(sql, [city, offset, limit])
        values = cursor.fetchall()
        for row in values:
            d = {'id': row[0],
                 'user': row[1],
                 'headimage': row[2],
                 'title': row[3],
                 'updatetime': row[4],
                 'coverimage': row[5]
                 }
            topic_list.append(d)
    except Exception, e:
        print 'database error', e
    finally:
        cursor.close()
        conn.close()

    return jsonify({'topic_list': topic_list})


@app.route('/api/detail/<topic_id>/', methods=['GET'])
def detail(topic_id):
    detail = {}
    try:
        conn = sqlite3.connect('./results/result_renthub.sqlite')
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM rent WHERE id=?', [topic_id])
        value = cursor.fetchone()
        str_value = value[4].encode('utf-8', errors='strict')
        detail = {'id': value[0],
                  'user': value[1],
                  'headimage': value[2],
                  'title': value[3],
                  'content': str_value,
                  'posttime': value[6],
                  'updatetime': value[7],
                  'source': value[9],
                  'note': value[10] if value[10] is not None else 0
                  }
    except Exception, e:
        print 'database error', e
        return
    finally:
        cursor.close()
        conn.close()

    return jsonify({'detail': detail})


@app.route('/api/fav/<user_id>/', methods=['GET'])
def get_fav(user_id):
    topic_list = []
    try:
        conn = sqlite3.connect('./results/rent-hub-fav.sqlite')
        cursor = conn.cursor()
        fav_str = cursor.execute('SELECT fav_str FROM favorite WHERE user_id = ? LIMIT ?,?', [user_id]).fetchone()

        if fav_str is not None:
            fav_list = fav_str[0].split(",")
            unique_fav_list = []
            for info_id in fav_list:
                if info_id not in unique_fav_list:
                    unique_fav_list.append(info_id)
            print "fav_list=", fav_list
            print "unique_fav_list=", unique_fav_list
        else:
            return jsonify({'favorite': topic_list})
        cursor.close()
        conn.close()

        conn_py = sqlite3.connect('./results/result_renthub.sqlite')
        cursor_py = conn_py.cursor()

        for info_id in unique_fav_list:
            cursor_py.execute("SELECT id, user, headimage, title, updatetime, coverimage FROM rent WHERE id= ?",
                              [info_id])
            row = cursor_py.fetchone()
            if row is None:
                continue
            d = {'id': row[0],
                 'user': row[1],
                 'headimage': row[2],
                 'title': row[3],
                 'updatetime': row[4],
                 'coverimage': row[5]
                 }
            topic_list.append(d)
        cursor_py.close()
        conn_py.close()
        return jsonify({'favorite': topic_list})
    except Exception, e:
        print 'database error', e
        return jsonify({'favorite': "error"})


@app.route('/api/addfav/', methods=['GET'])
def add_fav():
    user_id = request.args.get('user_id')
    info_id = request.args.get('info_id')
    try:
        conn = sqlite3.connect('./results/rent-hub-fav.sqlite')
        cursor = conn.cursor()
        value = cursor.execute('SELECT * FROM favorite WHERE user_id = ?', [user_id]).fetchone()

        print "value=", value

        if value is not None:
            pre = value[1]
            fav_str = pre.encode("utf-8") + "," + info_id.encode("utf-8")
            cursor.execute('UPDATE favorite SET fav_str=? WHERE user_id=?', [fav_str, user_id])
        else:
            fav_str = info_id.encode("utf-8")
            cursor.execute('INSERT INTO favorite(user_id, fav_str) VALUES (?, ?)', [user_id, fav_str])
        cursor.close()
        conn.commit()
        conn.close()
        return jsonify({'detail': fav_str})
    except Exception, e:
        print 'database error', e
        return jsonify({'detail': "error"})


@app.route('/api/dev1/', methods=['GET'])
def dev1():
    try:
        conn = sqlite3.connect('./results/rent-hub-fav.sqlite')
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS favorite(user_id TEXT PRIMARY KEY, fav_str TEXT)')
        cursor.close()
        conn.close()
        return jsonify({'detail': "dev1 ok"})
    except Exception, e:
        print 'database error', e
        return jsonify({'detail': "dev1 error"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)

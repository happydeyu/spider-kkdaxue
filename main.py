import requests
import json
import datetime
import time
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/78.0.3904.108 Safari/537.36 QIHU 360EE'
}

def insert(sql, cursor, db):
    try:
        #执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
        # print('更新')
    except:
        # 如果发生错误则回滚
        print('回滚')
        db.rollback()
def task():
      
    db = pymysql.connect(host='',
                        user='root',
                        password='',
                        port=3306,
                        database='suggest_data')
    cursor = db.cursor() 

    sel_sql = "select create_time from suggest order by create_time desc limit 2"
    cursor.execute(sel_sql)
    result = cursor.fetchall()
    t = result[0][0]

    sel_sql = "select create_time from suggest order by create_time limit 1"

    flag = 0
    for i in range(1000000):
        res = requests.get('https://www.kkdaxue.com/api/post/list/page?current=' + str(i+1) + '&pageSize=10&reviewStatus=1&sortField=createTime&sortOrder=descend')
        json_data = json.loads(res.text)
        data = json_data['data']['records']

        for d in data:
            
            cre = datetime.datetime.strptime(d['createTime'], '%Y-%m-%dT%H:%M:%S.000+00:00')
            if cre < t:
                flag = 1
                break
            upd = datetime.datetime.strptime(d['updateTime'], '%Y-%m-%dT%H:%M:%S.000+00:00')
            sql = "insert into suggest(education, school, major, work_exp,content,create_time, update_time )\
                    values('%s', '%s', '%s', '%s', '%s', '%s', '%s')"%(d['education'], d['school'], d['major'],d['workExp'],d['content'], cre, upd)
            # print(sql)
            insert(sql, cursor, db)
        time.sleep(0.25)
        if flag == 1:
            break

sched = BlockingScheduler()
sched.add_job(task, 'cron', day='*', hour=5, minute=36)
task()
sched.start()

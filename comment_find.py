import asyncio
from os import replace
from bilibili_api import video, Credential, comment, dynamic, user
import time
import pymysql
import datetime
import random
import re
import math
import logging

count = 0
listen_user_list = [703007996,672353429,672342685,672328094,672346917]
credential_listen = Credential(sessdata=r'xx', bili_jct=r'xx', buvid3=r'xx')

logger = logging.getLogger(__name__)
logger.setLevel(level = logging.INFO)
handler = logging.FileHandler("./log.txt", encoding='utf-8')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
db = pymysql.connect(host = '101.34.122.xx', port = 3306, user = "root", passwd = "xx", charset = 'utf8mb4')
cursor = db.cursor()

timesleep = 5 #爬完一页的休息时间，过快容易被ban，实验发现3s也可

def execute_sql(order):
    global db
    global logger
    try:
        cursor.execute(order)
        db.commit()
        logger.info("execute_sql success:")
    except Exception as e:
        logger.error("execute_sql failed:" + str(e))
        logger.error(str(order))
        db.rollback()

def comment_sql_order_makeup(is_flush, sql_order, count, user_id, reply_id, dynamic_id, up_num, comments_num, content, timestamp): #每x轮凑成一个语句提交至数据库
    global logger
    if(count >= 6 or is_flush):
        sql_head = """
        replace into bilibili.user_comment(
        user_id, reply_id, dynamic_id, up_num, 
        comments_num, content, time) values
        """
        sql_order += " ;"
        logger.info("insert info:" + sql_head + sql_order)
        execute_sql(sql_head + sql_order)
        sql_order = ""
        count = 0
        if(is_flush):
            return "", 0

    if(len(content) >= 1024):
        content = content[:1000]

    if(count != 0):
        sql_order += ","
    sql_new = "(\"" + str(user_id) + "\", \"" + \
            str(reply_id) + "\", \"" + \
            str(dynamic_id) + "\", " + \
            str(up_num) + ", " + \
            str(comments_num) + ", \"" + \
            str(content) + "\", \"" + \
            str(timestamp) + "\")"
    logger.info("get new insert content" + sql_new)

    sql_order += sql_new
    count += 1
    return sql_order,count

def dynamic_sql_order_makeup(dynamic_id, owner_id, timestamp, up_num, content, dynamic_type, rid):
    sql_order = """
        replace into bilibili.dynamic(
        dynamic_id, owner_id, timestamp, up_num, 
        content, dynamic_type, rid) value
        """
    
    if(len(content) >= 1024):
        content = content[:1000]

    sql_new = "(\"" + str(dynamic_id) + "\", \"" + \
            str(owner_id) + "\", \"" + \
            str(timestamp) + "\", " + \
            str(up_num) + ", \"" + \
            str(content) + "\", " + \
            str(dynamic_type) + ", \"" + \
            str(rid) + "\");"
    global logger
    logger.info("get new insert content: " + sql_order + sql_new)
    execute_sql(sql_order + sql_new)

async def check_is_rid(DV, rid):
    global timesleep
    global logger
    FORM_CHOICE = 0
    type_list = [17, 12, 11, 1, 2, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16, 18, 19, 20, 21, 22, 33]
    comments = []
    while(1):
        try:
            comments = await comment.get_comments(oid=int(DV), type_=type_list[FORM_CHOICE], page_index=1, credential=credential_listen)
            time.sleep(timesleep)
        except Exception as e:
            exception_str = str(e)
            if('404' in exception_str):
                time.sleep(random.randint(0, timesleep))
                if( FORM_CHOICE == len(type_list) - 1 ):
                    break
                else:
                    FORM_CHOICE += 1
                    continue
            elif('412' in exception_str):
                solve_412()
            else:
                logger.error("return code not 404----" + str(e))
                return False, -1
        return False, type_list[FORM_CHOICE]
    FORM_CHOICE = 0
    while(1):
        try:
            comments = await comment.get_comments(oid=int(rid), type_=type_list[FORM_CHOICE], page_index=1, credential=credential_listen)
            time.sleep(timesleep)
        except Exception as e:
            exception_str = str(e)
            if('404' in exception_str):
                time.sleep(random.randint(0, timesleep))
                if(FORM_CHOICE==len(type_list)-1):
                    break
                else:
                    FORM_CHOICE += 1
                    continue
            elif('412' in exception_str):
                time.sleep(60 + random.randint(10,20))
            else:
                logger.error("return code not 404----" + str(e))
                return False, -1
        return True, type_list[FORM_CHOICE]

async def remark_dynamic(dynamic_id, type):
    if(type):
        str1 = "update bilibili.dynamic  set is_search = 1 where rid = " + str(dynamic_id) + ";"
    else:
        str1 = "update bilibili.dynamic  set is_search = 1 where dynamic_id = " + str(dynamic_id) + ";"
    execute_sql(str1)

async def get_dynamic_id(listen_user, if_first):
    global logger
    timespace = 5
    if(if_first):
        timespace = 300
    for t in listen_user:
        offset = 0
        logger.info("uid:" + str(t.uid) + ",finding dynamics")
        while(1):
            temp = await t.get_dynamics(offset=offset)
            now_sec = time.time()
            if('cards' in temp.keys()):
                cards = temp['cards']
                for dynamic in cards:
                    id = dynamic['desc']['dynamic_id']
                    timestamp = dynamic['desc']['timestamp']
                    rid = dynamic['desc']['rid']
                    owner_id = t.uid
                    up_num = dynamic['desc']['like']
                    try:
                        if('item' in dynamic['card'].keys()):
                            if('content' in dynamic['card']['item'].keys()):    
                                content = dynamic['card']['item']['content']
                            elif('description' in dynamic['card']['item'].keys()):
                                content = dynamic['card']['item']['description']
                        elif('title' in dynamic['card'].keys()):
                            content = dynamic['card']['title']
                            if('summary' in dynamic['card'].keys()):
                                content += dynamic['card']['summary']
                        else:
                            content = ""
                    except Exception as e:
                        content = ""
                        logger.error(str(e))
                        logger.error(str(id) + dynamic)
                    if(abs(now_sec - timestamp) <= 86400*timespace):
                        is_rid, types = await check_is_rid(id, rid)
                        if(types == -1):
                            continue
                        if(is_rid):
                            types = -types
                        dynamic_sql_order_makeup(id, owner_id, timestamp, up_num, content, types, rid)
                    else:
                        break
                offset = int(temp['next_offset'])
            else:
                break

async def get_user_all_dynamics(listen_user_list, is_init):
    for i in range(len(listen_user_list)):
        listen_user_list[i] = user.User(str(listen_user_list[i]), credential_listen)
    await get_dynamic_id(listen_user_list, is_init)

def solve_412():  #解决接口访问速度过快，被cr拒绝的问题，我没有其他的ip可用，所以选择睡眠半小时。不知道可不可行
    global logger
    logger.warning("412 to sleep")
    time.sleep(60*60*0.5)
    
async def get_dynamic_comments(is_init):
    global logger
    timespace = 3
    if(is_init):
        timespace = 300
    is_rid = False
    timestamp = time.time()
    timestamp = timestamp - 86400 * timespace
    sql_order = "SELECT * from bilibili.dynamic where timestamp >= " + str(timestamp) + " ORDER BY timestamp desc;"
    result = select_sql(sql_order)
    if(result == ""):
        return
    for item in result:
        dynamic_id, owner_id, timestamp, up_num, content, type, rid, is_search = item
        if(is_search == 1):
            continue
        if(type < 0):
            dynamic_id = rid
            type = -type
            is_rid = True
        elif(type > 0):
            dynamic_id = dynamic_id
        else:
            logger.error("dynamic type = 0")
        i = 1
        page_max = 1
        try:
            comments = await comment.get_comments(oid=int(dynamic_id), type_=type, page_index=i, credential=credential_listen)
            time.sleep(timesleep)
            page_max = math.ceil(int(comments['page']['count'])/int(comments['page']['size']))
            logger.info("dynamic:" + dynamic_id + ", page_max = " + str(page_max))
        except Exception as e:
            logger.error("get page max failed" + str(e))
            if("412" in str(e)):
                solve_412()
            continue
        while(i <= page_max):
            sql_order = ""
            count = 0
            try:
                comments = await comment.get_comments(oid=int(dynamic_id), type_=type, page_index=i, credential=credential_listen)
                if(comments['replies']!=None):
                    for t in comments['replies']:
                        sql_order, count = comment_sql_order_makeup(False, sql_order, count, t['mid'], t['rpid'], dynamic_id, t['like'], 0, t['content']['message'], t['ctime'])
                else:
                    break
            except Exception as e:
                if('412' in str(e)):
                    solve_412()
            if(count != 0):
                try:
                    sql_order, count = comment_sql_order_makeup(is_flush=True, sql_order=sql_order, count=0, user_id=0, reply_id=0, dynamic_id=0, up_num=0, comments_num=0, content=0, timestamp=0)
                except Exception as e:
                    logger.error(str(e))
            i += 1
        time.sleep(random.randint(25,30)/10 * timesleep)
        await remark_dynamic(dynamic_id, is_rid) #标记动态评论已爬完

def select_sql(sql_order):
    global db
    global logger
    try:
        cursor.execute(sql_order)
        logger.info("execute_sql success:" + str(sql_order))
        return cursor.fetchall()
    except Exception as e:
        logger.error("execute_sql failed:" + str(e))
        logger.error(str(sql_order))
        return ""

async def main():

    global listen_user_list
    global count
    '''
    if(check_is_fobidden()):
        solve_412()
    else:
        time.slepp(60*30)
    count += 1
    '''
    #await get_user_all_dynamics(listen_user_list, True)
    #if(count % 300 == 0):
    await get_user_all_dynamics(listen_user_list, True) #爬取指定用户动态相关信息
    await get_dynamic_comments(True) #获取动态评论
    

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())


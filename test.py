#!/usr/bin/env python
# coding=UTF-8
'''
@Author: qi-you
@Date: 2020-02-19 17:50:28
@LastEditors: qi-you
@LastEditTime: 2020-02-25 15:31:43
@Descripttion: 
'''

'''
下载文件数据并筛选数据插入到数据库
    1、从url中获取文件
    2、从文件中筛选选数据
    3、插入数据库:全部插入(先删除全部数据再插入？)、更新插入
    需要添加:使用用户名和账号密码连接
    文件一：1061
    （1）employeenumber对应：Worker_ID
    （2）uid 对应：Email_Address
    （5）ntUserDomainId  对应：NT_User_Domain_ID
    需要Replace 'AUTH:' with 'AUTH\'样例AUTH:dsnod -->AUTH\dsnod
    (6) hpStartDate 对应 HP_Start_Date
    （7）hpStatus 对应 HP_Status
    (10) cn 对应：cn
    （56） hpTerminationDate 对应 HP_Termination_Date
    （57） modifytimestamp 对应 Modify_Timestamp
    文件二：1065
    （1）employeenumber对应：Worker_ID
    （2）uid 对应：Email_Address
    （5）ntUserDomainId  对应：NT_User_Domain_ID
    需要Replace 'AUTH:' with 'AUTH\'样例AUTH:dsnod -->AUTH\dsnod
    (6) hpStartDate 对应 HP_Start_Date
    （7）hpStatus 对应 HP_Status
    (9) cn 对应：cn
    (53) hpExpectedEndDate 对应 HP_Expected_End_Date
    （54） hpTerminationDate 对应 HP_Termination_Date
    （55） modifytimestamp 对应 Modify_Timestamp
'''

import gzip
import logging
import sys
import time
from datetime import datetime
import urllib
import pyodbc
import requests
import configparser
import uuid
from Encrpt import PrpCrypt
url_pwd = None
url_user = None
user_name = None
user_password = None
logger = None
database = ''
server_name = ''


def download_file(fileurl):
    '''
    @description: 下载文件读数据
    @param {str} 
    @return: None
    '''
    global logger
    is_hp = 1
    data = []
    
    conn_info = "DRIVER={ODBC Driver 17 for SQL Server};DATABASE="+database+";SERVER="+server_name+";UID="+user_name+";PWD="+user_password+";"
    cnxn = pyodbc.connect(conn_info)
    cursor = cnxn.cursor()
    cursor.fast_executemany = True
    truncate(cursor)
    for url in fileurl:
        logger.info(f"download file{is_hp}")
        # f = urllib.request.urlopen(url)
        # open(f"download\\urlopen{is_hp}.zip",'wb').write(f.read())
        gzr = gzip.open(f"download\\urlopen{is_hp}.zip", 'r')
        data = generated_sql(gzr, is_hp)
        insert_sql(data, is_hp,cursor)
        logger.info(f"total: {len(data)}")
        is_hp = 0
    logger.info(f"compare data")
    compare(cursor)

def truncate(cursor):
    logger.info(f"TRUNCATE TABLE [IT_OPS].[ED_LZ_Employee]")
    cursor.execute("delete [IT_OPS].[ED_LZ_Employee];")
    cursor.commit()
    logger.info(f"TRUNCATE TABLE [IT_OPS].[ED_LZ_Employee] successful")


def generated_sql(r01, is1061):
    '''
    @description: 根据文件获取出value
    @param {file,bool} 
    @return: list
    '''
    global logger
    file_data = []
    insert_rows = []
    for r in r01.readlines():
        file_data.append(r.decode("utf-8").split("\t"))
    r01.close()
    logger.info(f"file{is1061} row {len(file_data)}")
    for lin in file_data:
        column_value = []
        column_value.append(lin[0])
        column_value.append(lin[1])
        column_value.append(lin[4].replace(":", "\\"))
        column_value.append(datetime.strptime(lin[5], '%Y-%m-%d'))
        column_value.append(lin[6])
        if is1061:
            column_value.append(lin[9].replace("'", "''"))
            if lin[55] == '':
                lin[55] = None
                column_value.append(lin[55])
            else:
                column_value.append(datetime.strptime(lin[55], '%Y-%m-%d'))
            column_value.append(datetime.strptime(lin[56], '%Y%m%d%H%M%SZ'))
        else:
            column_value.append(lin[8].replace("'", "''"))
            if lin[52] == '':
                lin[52] = None
                column_value.append(lin[52])
            else:
                column_value.append(datetime.strptime(lin[52], '%Y-%m-%d'))
            if lin[53] == '':
                lin[53] = None
                column_value.append(lin[53])
            else:
                column_value.append(datetime.strptime(lin[53], '%Y-%m-%d'))
            column_value.append(datetime.strptime(lin[54], '%Y%m%d%H%M%SZ'))
        insert_rows.append(column_value)
    return insert_rows


def insert_sql(table, is_hp,cursor):
    '''
    @description: 插入数据库
    @param {list} 
    @return: None
    '''
    global logger
    column = "Worker_ID,Email_Address,NT_User_Domain_ID,HP_Start_Date,HP_Status,CN,HP_Expected_End_Date,HP_Termination_Date,Modify_Timestamp,Is_HP,Update_GMT_Timestamp"
    HP_Expected_End_Date = "null" if is_hp else "?"
    sql = f"insert into [IT_OPS].[ED_LZ_Employee]({column}) values(?,?,?,?,?,?,{HP_Expected_End_Date},?,?,{is_hp},getdate())"
    # 批量插入
    # try:
    logger.info(f"insert into number {len(table)}")
    logger.info("insert into [IT_OPS].[ED_LZ_Employee]........")
    cursor.executemany(sql, table)
    cursor.commit()
    logger.info(f"Insertion successful number {len(table)}")
    # except:
    #     logger.info(f"Insertion error:insertion failed")
    # finally:
    #     cursor.close()
    #     logger.info("Close the connection")


def getlogg(name):
    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    rf_handler = logging.StreamHandler(sys.stderr)  # 默认是sys.stderr
    rf_handler.setLevel(logging.DEBUG)
    rf_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(name)s - %(message)s"))
    f_handler = logging.FileHandler('log.log')
    f_handler.setLevel(logging.INFO)
    f_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
    logger.addHandler(rf_handler)
    logger.addHandler(f_handler)


def read_config(config_file='Download_file_insert_SQLServer.ini'):
    '''
    @description: 
    @param {type} 
    @return: 
    '''
    global database, server_name, user_password, user_name, url_pwd, url_user
    cf = configparser.ConfigParser()
    cf.read(config_file)
    env = cf.get('baseconf', 'env')
    # 加密判断
    p = PrpCrypt(str('qi-you' * 5)[:16])
    database = cf['baseconf']['database']
    server_name = cf['baseconf']['server_name']
    for change in cf[env]:
        if cf[env][change] != cf['old'][change]:
            tmp = cf[env][change]
            cf.set(env, change, str(p.encrypt(tmp), 'utf-8'))
            cf.set('old', change, str(p.encrypt(tmp), 'utf-8'))
    with open(config_file, "w+") as f:
        cf.write(f)
    url_pwd = p.decrypt(cf[env]['url_pwd'])
    url_user = p.decrypt(cf[env]['url_user'])
    user_name = p.decrypt(cf[env]['user_name'])
    user_password = p.decrypt(cf[env]['user_password'])
    return


def main():
    getlogg(__name__)
    global logger
    starttime = time.time()
    logger.info("start")
    read_config()
    url = [f"https://g1t6002g.austin.hpicorp.net/download/download.cgi?auth_name={url_user}%3DSRVC_GS_Directory_Readonly%2Cou%3DApplications%2Co%3Dhp.com&auth_passwd={url_pwd}%23123&action=download_std&fileid=1061&compress=1",
           f"https://g1t6002g.austin.hpicorp.net/download/download.cgi?auth_name={url_user}%3DSRVC_GS_Directory_Readonly%2Cou%3DApplications%2Co%3Dhp.com&auth_passwd={url_pwd}%23123&action=download_std&fileid=1065&compress=1"]
    download_file(url)
    end = time.time()
    runtime = end-starttime
    logger.info(f"Running time:{runtime}")


def compare(cursor):
    global logger
    cursor.execute("select top 1 * [IT_OPS].[ED_Dim_Employee]")
    if len(list(cursor.fetchall()))>0:
        sql = f"insert into [IT_OPS].[ED_Dim_Employee] values(?,?,?,?,?,?,?,?,?,?,?)"
        cursor.execute('''SELECT Worker_ID,Email_Address,NT_User_Domain_ID,HP_Start_Date,HP_Status,CN,HP_Expected_End_Date,HP_Termination_Date
                        FROM (
                            SELECT * FROM [IT_OPS].[ED_LZ_Employee]
                            UNION ALL
                            SELECT * FROM [IT_OPS].[ED_Dim_Employee]
                        ) tbl
                        GROUP BY 
                        Worker_ID,Email_Address,NT_User_Domain_ID,HP_Start_Date,HP_Status,CN,HP_Expected_End_Date,HP_Termination_Date
                        HAVING count(*) = 1
                        ORDER BY Worker_ID;''')
        newdata = list(cursor.fetchall())
        if len(newdata)==0:
            logger.info("no change")
            return
        change_worker_id = set()
        lz_change = set()
        for i in range(len(newdata)):
            change_worker_id.add(newdata[i][0])
        logger.info(f"change Worker_ID {str(change_worker_id)}")
        cursor.execute(f'''select * from [IT_OPS].[ED_LZ_Employee] where Worker_ID in ('{"','".join(change_worker_id)}')''')
        lz_compare = list(cursor.fetchall())
        if len(lz_compare)>0:
            for i in range(len(lz_compare)):
                lz_change.add(lz_compare[i][0])
            logger.info(f"delete [IT_OPS].ED_Dim_Employee Worker_ID{str(change_worker_id)}")
            delete_dim = f'''delete [IT_OPS].ED_Dim_Employee where Worker_ID in ('{"','".join(list(change_worker_id))}')'''
            cursor.execute(delete_dim)
            logger.info(f"insert [IT_OPS].ED_Dim_Employee new Worker_ID{str(lz_compare)}")
            cursor.executemany(sql,lz_compare)
        else:
            logger.info(f"delete [IT_OPS].ED_Dim_Employee Worker_ID{str(change_worker_id)}")
            delete_dim = f'''delete [IT_OPS].ED_Dim_Employee where Worker_ID in ('{"','".join(list(change_worker_id))}')'''
            cursor.execute(delete_dim)
        cursor.commit()
    else:
        cursor.execute("insert into [IT_OPS].ED_Dim_Employee select  * from [IT_OPS].[ED_LZ_Employee]")
        cursor.commit()
        


if __name__ == "__main__":
    main()

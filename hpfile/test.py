#!/usr/bin/env python
# coding=UTF-8
'''
@Author: qi-you
@Date: 2020-03-18 16:23:58
@LastEditors: qi-you
@LastEditTime: 2020-03-31 17:13:31
@Descripttion:
'''

import logging
import sys
import time
import pandas as pd
from pandas import DataFrame
from datetime import datetime
import configparser
import uuid
import os
from sqlalchemy import create_engine
from Encrpt import PrpCrypt
from logging_config import setup_logging
import SQL_Tool
user_name = None
user_password = None
database_name = None
server_name = None
path = None
sheet_name = None
driver = None


def read_config(config_file='config.ini'):
    '''
    @description:读取配置文件
    @param {type}
    '''
    cf = configparser.ConfigParser()
    cf.read(config_file)
    env = cf.get('baseconf', 'env')
    execl = cf.get('baseconf', 'node')
    global server_name
    server_name = cf[env]['server_name']

    global database_name
    database_name = cf[env]['database']

    global driver
    driver = cf[env]['driver']

    global path
    path = cf[execl]['path']

    global sheet_name
    sheet_name = cf[execl]['sheet_name']
    # 加密判断
    encryption = cf[env]['encryption']

    mac = uuid.UUID(int=uuid.getnode()).hex[-12:]
    p = PrpCrypt(str(mac * 5)[:16])

    if encryption == "False":
        name = cf[env]['user_name']
        password = cf[env]['user_password']
        encrpt_name = str(p.encrypt(name), 'utf-8')
        encrpt_password = str(p.encrypt(password), 'utf-8')
        # 写入ini文件
        cf.set(env, "user_name", encrpt_name)
        cf.set(env, "user_password", encrpt_password)
        cf.set(env, "encryption", "True")
        with open(config_file, "w+") as f:
            cf.write(f)
    global user_name
    user_name = p.decrypt(cf[env]['user_name'])
    # user_name = ""
    global user_password
    # user_password = ""
    user_password = p.decrypt(cf[env]['user_password'])
    return


def main():
    starttime = time.time()
    logger.info("start")
    read_config('config.ini')
    sqlserver = SQL_Tool.SQL(
        'SQL Server', user_name, user_password, server_name, database_name, driver=driver)
    # -----------start---------
    execl_column = ['Global Region',
                    'Transaction Date',
                    'CFA Code',
                    'Business Process Desc',
                    'Shipping Plant',
                    'Country Desc',
                    'Part No',
                    'Order No',
                    'PGI DATE TIME',
                    'Order Line Create Datetime',
                    'Pickable CO Datetime',
                    'Shippable CO Datetime',
                    'Requested Delivery Datetime',
                    'Order Line',
                    'LOF-Net Qty',
                    'LOF-Ship Qty',
                    'LOF-Miss Qty',
                    'Shipping Condition',
                    'Order Type',
                    'Order Reason',
                    'WFM Case ID',
                    'HWPL Code',
                    'Function Group Code',
                    'Miss Code',
                    'Delivery Priority',
                    'Ship To Code',
                    'Ship To Customer Name',
                    'Created by',
                    'Product',
                    'Sub Title',
                    'Case Otc',
                    'Sub Otc',
                    'Customer Name',
                    'Address',
                    'Address2',
                    'State',
                    'Owner',
                    'Miss root cause group',
                    'Miss root cause ',
                    'notes', 'KEY']
    table_column = ['Global_Region',
                    'Transaction_Date',
                    'CFA_Code',
                    'Business_Process_Desc',
                    'Shipping_Plant',
                    'Country_Desc',
                    'Part_No',
                    'Order_No',
                    'PGI_DATE_TIME',
                    'Order_Line_Create_Datetime',
                    'Pickable_CO_Datetime',
                    'Shippable_CO_Datetime',
                    'Requested_Delivery_Datetime',
                    'Order_Line',
                    'LOF-Net_Qty',
                    'LOF-Ship_Qty',
                    'LOF-Miss_Qty',
                    'Shipping_Condition',
                    'Order_Type',
                    'Order_Reason',
                    'WFM_Case_ID',
                    'HWPL_Code',
                    'Function_Group_Code',
                    'Miss_Code',
                    'Delivery_Priority',
                    'Ship_To_Code',
                    'Ship_To_Customer_Name',
                    'Created_by',
                    'Product',
                    'Sub_Title',
                    'Case_Otc',
                    'Sub_Otc',
                    'Customer_Name',
                    'Address',
                    'Address2',
                    'State',
                    'Miss_Root_Cause_Group',
                    'Miss_Root_Cause',
                    'Owner',
                    'Notes',
                    'KEY']

    # 读取execl文件
    df = pd.read_excel(path, sheet_name=sheet_name,
                       usecols=execl_column)
    # 修改列名
    df.columns = table_column
    execl_key = set()
    # 计算key值
    for index in range(df.index.stop):
        df['KEY'][index] = str(df['Order_No'][index])+"-"+str(df['Part_No'][index]) + \
            "-"+str(df['Order_Line'][index])+'-' + \
            datetime.strftime(df['Transaction_Date'][index], '%Y%m%d')
        execl_key.add(df['KEY'][index])
    # engine = create_engine(
    #     f"mssql+pyodbc://{user_name}:{user_password}@{server_name}/{database_name}?driver={driver}", encoding='utf-8', fast_executemany=True)
    # if len(engine.execute('select * from IT_OPS.XQY_TGT_TEST2').fetchall()) == 0:
    logger.info("select table data")
    if len(sqlserver._query('select * from IT_OPS.XQY_TGT_TEST2')) == 0:
        logger.info('table no data')
        logger.info(f'start insert into {len(df)} rows')
        sqlserver._insert(df, 'XQY_TGT_TEST2', 'IT_OPS')
        # df.to_sql(name='XQY_TGT_TEST2', con=engine,
        #           if_exists='append', schema='IT_OPS', index=False, chunksize=1000)
        logger.info(f'Insertion successful number {len(df)}')
    else:
        # 获取旧的key
        # old_key = set(pd.read_sql_query(
        #     'select [KEY] from IT_OPS.XQY_TGT_TEST2', engine)['KEY'])
        logger.info("Comparative data")
        old_key = set(sqlserver._query(
            'select [KEY] from IT_OPS.XQY_TGT_TEST2')['KEY'])
        add_key = list(execl_key-old_key)
        if len(add_key) > 0:
            logger.info(f"new key:{add_key}")
            new_df = DataFrame(columns=table_column)
            for key in add_key:
                new_df = new_df.append(
                    df.loc[df['KEY'] == key], ignore_index=True)
            logger.info(f'insert into new data number {len(new_df)}')
            # new_df.to_sql(name='XQY_TGT_TEST2', con=engine,
            #               if_exists='append', schema='IT_OPS', index=False, chunksize=1000)
            sqlserver._insert(new_df, 'XQY_TGT_TEST2', 'IT_OPS')
            logger.info(f'Insertion successful number {len(new_df)}')
        else:
            logger.info('no change data')
    end = time.time()
    runtime = end-starttime
    logger.info(f"Running time:{runtime}")


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    try:
        setup_logging(default_path='logging.json',
                      default_level=logging.INFO,
                      env_key='LOG_CFG')
        main()
    except Exception as e:
        logger.error(e)

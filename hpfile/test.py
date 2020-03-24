#!/usr/bin/env python
# coding=UTF-8
'''
@Author: qi-you
@Date: 2020-03-18 16:23:58
@LastEditors: qi-you
@LastEditTime: 2020-03-23 20:51:57
@Descripttion:
'''

import logging
import sys
import time
import pandas as pd
from pandas import DataFrame
from datetime import datetime
import pyodbc
import configparser
import uuid
import os
from sqlalchemy import create_engine
from Encrpt import PrpCrypt
user_name = None
user_password = None
database_name = None
server_name = None
path = None
sheet_name = None
driver = None
logger = None


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


def read_config(config_file='config.ini'):
    '''
    @description:
    @param {type}
    @return:
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
    getlogg(__name__)
    starttime = time.time()
    logger.info("start")
    # -----------start---------
    column = ["Global_Region",
              "Transaction_Date",
              "CFA_Code",
              "Business_Process_Desc",
              "Delivery_Model_Desc",
              "Shipping_Plant",
              "Country_Desc",
              "Part_No",
              "Part_Desc",
              "Order_No",
              "PGI_DATE_TIME",
              "Order_Line_Create_Datetime",
              "Pickable_CO_Datetime",
              "Shippable_CO_Datetime",
              "Requested_Delivery_Datetime",
              "Order_Line",
              "LOF_Net_Qty",
              "LOF_Ship_Qty",
              "LOF_Miss_Qty",
              "Shipping_Condition",
              "Order_Type",
              "Order_Reason",
              "WFM_Case_ID",
              "HWPL_Code",
              "Function_Group_Code",
              "Miss_Code",
              "Delivery_Priority",
              "Ship_To_Code",
              "Ship_To_Customer_Name",
              "wk",
              "Created_by",
              "PL30_to_PLTW",
              "CP_or_UP",
              "Service_Type_Description",
              "Mapping",
              "LSA_Code",
              "Consumables_Exclusion",
              "Case_Id",
              "Subcase_Id",
              "Product",
              "Product_Line",
              "Case_Title",
              "Sub_Title",
              "Case_Otc",
              "Sub_Otc",
              "Sub_Summary_Text",
              "Customer_Name",
              "Address",
              "Address2",
              "State",
              "Month",
              "Miss_Root_Cause_Group",
              "Miss_Root_Cause ",
              "Owner",
              "notes",
              "KEY"]
    read_config('config.ini')
    df = pd.read_excel(path, sheet_name=sheet_name, names=column)
    engine = create_engine(
        f"mssql+pyodbc://{user_name}:{user_password}@{server_name}/{database_name}?driver={driver}", encoding='utf-8', fast_executemany=True)
    df.to_sql(name='XQY_TEST2', con=engine,
              if_exists='append', schema='IT_OPS', index=False, chunksize=1000)
    end = time.time()
    runtime = end-starttime
    logger.info(f"Running time:{runtime}")


if __name__ == "__main__":
   
    # read_config("config.ini")
    main()
    # df = pd.read_excel(path, sheet_name=sheet_name)
    # columnlist = [column for column in df]
    # data = {}
    
    # for column in columnlist:
    #     i = 0
    #     map = ''
    #     for v in df[column]:
    #         # print(len(v))
    #         # break
    #         if len(str(v))>i:
    #             i = len(str(v))
    #             map= v
    #     data.setdefault(column+str(i),map)

    # #     # break
    # print(data)
    # for i in maxdata:
    #     print(len(str(i)))
    # maxdata.to_csv("max_column1.csv")
    # os.system(r"net use E: \\ZHOUYIS3\Users\ZhouYiS\Desktop\ExcelCompare_jar")
    # except Exception as e:
    #     logger.error(e)

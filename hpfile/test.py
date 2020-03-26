#!/usr/bin/env python
# coding=UTF-8
'''
@Author: qi-you
@Date: 2020-03-18 16:23:58
@LastEditors: qi-you
@LastEditTime: 2020-03-26 19:02:53
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
              "LOF-Net_Qty",
              "LOF-Ship_Qty",
              "LOF-Miss_Qty",
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
    for index in range(df.index.stop):
        df['KEY'][index] = str(df['Order_No'][index])+"-"+str(df['Part_No'][index]) + \
            "-"+str(df['Order_Line'][index])+'-' + \
            str(df['Transaction_Date'][index])
    engine = create_engine(
        f"mssql+pyodbc://{user_name}:{user_password}@{server_name}/{database_name}?driver={driver}", encoding='utf-8', fast_executemany=True)
    df.to_sql(name='XQY_TEST2', con=engine,
              if_exists='append', schema='IT_OPS', index=False, chunksize=1000)
    if len(engine.execute('select * from IT_OPS.XQY_TGT_TEST2').fetchall()) == 0:
        engine.execute('''insert into IT_OPS.XQY_TGT_TEST2 select [Global_Region],
                    [Transaction_Date],
                    [CFA_Code],
                    [Business_Process_Desc],
                    [Shipping_Plant],
                    [Country_Desc],
                    [Part_No],
                    [Order_No],
                    [PGI_DATE_TIME],
                    [Order_Line_Create_Datetime],
                    [Pickable_CO_Datetime],
                    [Shippable_CO_Datetime],
                    [Requested_Delivery_Datetime],
                    [Order_Line],
                    [LOF-Net_Qty],
                    [LOF-Ship_Qty],
                    [LOF-Miss_Qty],
                    [Shipping_Condition],
                    [Order_Type],
                    [Order_Reason],
                    [WFM_Case_ID],
                    [HWPL_Code],
                    [Function_Group_Code],
                    [Miss_Code],
                    [Delivery_Priority],
                    [Ship_To_Code],
                    [Ship_To_Customer_Name],
                    [Created_by],
                    [Product],
                    [Sub_Title],
                    [Case_Otc],
                    [Sub_Otc],
                    [Customer_Name],
                    [Address],
                    [Address2],
                    [State],
                    [Owner],
                    [Miss_root_cause_group],
                    [Miss_root_cause],
                    [Notes] from IT_OPS.XQY_TGT_TEST2
                    ''')
    
    # if len(list(engine.fetchall())) ==0:
    #     print("no")
    end = time.time()
    runtime = end-starttime
    logger.info(f"Running time:{runtime}")


if __name__ == "__main__":

    # read_config('config.ini')
    main()
    # except Exception as e:
    #     logger.error(e)
    # df.row

    # df = pd.read_excel(r'.\file\table.xlsx', sheet_name="Sheet1",converters ={'Lenght':int})
    # df.iloc[a,3]
    # for s in list(df[0]):
    # a = df.loc[df['Column Name']==s].index.values[0]
    # print(s.replace(" ","_")+f" nvarchar({df.iloc[a,3]}) null,")
    # tag = [0, 1, 2, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
    #        23, 24, 25, 26, 27, 28, 30, 39, 42, 43, 44, 46, 47, 48, 49, 53, 51, 52, 54]
    # column1 = ['Global_Region',
    #           'Transaction_Date',
    #           'CFA_Code',
    #           'Business_Process_Desc',
    #           'Shipping_Plant',
    #           'Country_Desc',
    #           'Part_No',
    #           'Order_No',
    #           'PGI_DATE_TIME',
    #           'Order_Line_Create_Datetime',
    #           'Pickable_CO_Datetime',
    #           'Shippable_CO_Datetime',
    #           'Requested_Delivery_Datetime',
    #           'Order_Line',
    #           'LOF-Net_Qty',
    #           'LOF-Ship_Qty',
    #           'LOF-Miss_Qty',
    #           'Shipping_Condition',
    #           'Order_Type',
    #           'Order_Reason',
    #           'WFM_Case_ID',
    #           'HWPL_Code',
    #           'Function_Group_Code',
    #           'Miss_Code',
    #           'Delivery_Priority',
    #           'Ship_To_Code',
    #           'Ship_To_Customer_Name',
    #           'Created_by',
    #           'Product',
    #           'Sub_Title',
    #           'Case_Otc',
    #           'Sub_Otc',
    #           'Customer_Name',
    #           'Address',
    #           'Address2',
    #           'State',
    #           'Owner',
    #           'Miss_root_cause_group',
    #           'Miss_root_cause',
    #           'Notes']
    # df = pd.read_excel(path, sheet_name=sheet_name, names=column, nrows=3)
    # for index in range(df.index.stop):
    #     df['KEY'][index] = str(df['Order_No'][index])+"-"+str(df['Part_No'][index]) + \
    #         "-"+str(df['Order_Line'][index])+'-' + \
    #         str(df['Transaction_Date'][index])
    # print(df)

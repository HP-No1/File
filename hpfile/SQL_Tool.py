#!/usr/bin/env python
# coding=UTF-8
'''
@Author: qi-you
@Date: 2020-03-30 14:03:05
@LastEditors: qi-you
@LastEditTime: 2020-03-31 09:37:29
@Descripttion: sql and vertica connection
'''
import logging
import vertica_python
import pandas as pd
from sqlalchemy import create_engine

class SQL(object):
    def __init__(self, sql_name, user_name, user_password, server_name, database_name,driver='SQL Server'):
        '''
        @description: 
        @param sql_name:需要连接的数据库名字
        @param user_name:用户名
        @param user_password:密码
        @param server_name:ip地址
        @param database_name:数据库名字 
        @param driver:连接驱动 

        @return: 
        '''
        self._sql_name = sql_name
        self._user_name = user_name
        self._user_password = user_password
        self._server_name = server_name
        self._database_name = database_name
        if sql_name == 'Vertica':
            self._engine = create_engine(
                f'vertica+vertica_python://{user_name}:{user_password}@{server_name}/{database_name}')
        else:
            self._engine = create_engine(
                f"mssql+pyodbc://{user_name}:{user_password}@{server_name}/{database_name}?driver={driver}", encoding='utf-8', fast_executemany=True)

    def _query(self, sql_str):
        '''
        @description: 查询数据放回DataFrame
        @param sql_str: 查询语句
        @return: DataFrame
        '''
        return pd.read_sql_query(sql_str, self._engine)

    def _insert(self, data_talbe, table_name,schema):
        '''
        @description: 把数据插入数据库
        @param data_talbe:DataFrame数据
        @param table_name:要插入表名字（不包括schema）
        @param schema:schema名字
        @return: 
        '''
        data_talbe.to_sql(name=table_name, con=self._engine,
                          if_exists='append', schema=schema, index=False, chunksize=1000)

    def _copy(self, path, copy_sql):
        '''
        @description: 使用copy的方式来插入（仅限于Vertica插入）
        @param copy_sql :样例："copy {schema.table}(columnstr) FROM STDIN DELIMITER ',' ENCLOSED BY '\"' commit;"
        @return: 
        '''
        conn_info = {
            'host': self._server_name,
            'port': 5433,
            'user': self._user_name,
            'password': self._user_password,
            'database': self._database_name,
            'log_level': logging.DEBUG,
            # 默认情况下会自动生成会话标签，
            'session_label': 'some_label',
            # 无效的UTF-8结果默认抛出错误
            'unicode_error': 'strict',
            # 默认情况下禁用SSL
            'ssl': False,
            # 默认情况下，禁用使用服务器端预处理语句
            'use_prepared_statements': False,
            # 默认情况下未启用连接超时
            # 套接字操作5秒超时（建立TCP连接或读/写操作）
            'connection_timeout': 5}
        # 使用copy的方式来插入
        # copy = "copy {parame.schema}.test_{parame.table}({columnstr}) FROM STDIN DELIMITER ',' ENCLOSED BY '\"' commit;"
        connection = vertica_python.connect(**conn_info)
        c = connection.cursor()
        with open(path, "rb") as fs:
            c.copy(copy_sql, fs, buffer_size=65536)
            c.execute("commit;")
        c.close()
        connection.close()


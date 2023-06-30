#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年6月28日17:37:53
# 通用类
# 功能： 导入述文件到hive数据库表
# 主要使用 pyhive
import base64
import warnings

# 屏蔽本条语句之后所有的告警（paramiko 告警报错）
warnings.filterwarnings("ignore")

import os
import json
import sys
import hdfs
import krbticket
from xml.etree import ElementTree as ET
import socket
import paramiko
from datetime import datetime, timedelta

# 数据库导入到MySQL
import utils.mysqlUtil as myut

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)


class DATALOADER(object):
    def __init__(self):
        self.BASE_DIR = BASE_DIR
        self.jsonfile_path = self.BASE_DIR + "/conf/dataload/dataload.json"
        name, version, usemysql, use_pwd_coding, user, port, host, password_encoding, \
            password, database, datalaod_hive_json, datalaod_hive_sql, datalaod_hdfs_json, \
            datalaod_hdfs_sql, datalaod_yarn_json, datalaod_yarn_sql,table_name,charset = self._json_parse()
        self.name = name
        self.version = version
        self.usemysql = usemysql
        self.use_pwd_coding = use_pwd_coding
        self.user = user
        self.port = port
        self.host = host
        self.password_encoding = password_encoding
        self.password = password
        self.database = database

        self.datalaod_hive_json = datalaod_hive_json
        self.datalaod_hive_sql = datalaod_hive_sql
        self.datalaod_hdfs_json = datalaod_hdfs_json
        self.datalaod_hdfs_sql = datalaod_hdfs_sql
        self.datalaod_yarn_json = datalaod_yarn_json
        self.datalaod_yarn_sql = datalaod_yarn_sql



        self.table_name = table_name
        self.charset = charset

        # 通过set方法写入sql列表
        # 通过get方法写入sql列表
        self.sqlslist = []

    # hive配置文件参数分析
    def _json_parse(self):
        # print(self.jsonfile_path)
        with open(self.jsonfile_path, 'r') as jsonfile:
            load_dict = json.load(jsonfile)

            name = load_dict["name"]
            version = load_dict["version"]

            # mysql 配置
            # usemysql = load_dict["use_mysql"]
            usemysql = load_dict["dependencies"]["config"]["use_mysql"]
            use_pwd_coding = load_dict["dependencies"]["mysql"]["use_pwd_coding"]
            user = load_dict["dependencies"]["mysql"]["user"]
            host = load_dict["dependencies"]["mysql"]["host"]
            port = load_dict["dependencies"]["mysql"]["port"]
            password_encoding = load_dict["dependencies"]["mysql"]["password_encoding"]
            password = load_dict["dependencies"]["mysql"]["password"]
            database = load_dict["dependencies"]["mysql"]["database"]
            table_name = load_dict["dependencies"]["mysql"]["table_name"]
            charset = load_dict["dependencies"]["mysql"]["charset"]

            datalaod_hive_json = load_dict["dependencies"]["mysql"]["dataload_hive"]["json_dic"]
            datalaod_hive_sql = load_dict["dependencies"]["mysql"]["dataload_hive"]["sql"]
            datalaod_hdfs_json = load_dict["dependencies"]["mysql"]["dataload_hdfs"]["json_dic"]
            datalaod_hdfs_sql = load_dict["dependencies"]["mysql"]["dataload_hdfs"]["sql"]
            datalaod_yarn_json = load_dict["dependencies"]["mysql"]["dataload_yarn"]["json_dic"]
            datalaod_yarn_sql = load_dict["dependencies"]["mysql"]["dataload_yarn"]["sql"]


            return name, version, usemysql, use_pwd_coding, user, port, host, password_encoding, \
                password, database, datalaod_hive_json, datalaod_hive_sql, datalaod_hdfs_json, \
                datalaod_hdfs_sql, datalaod_yarn_json, datalaod_yarn_sql,table_name,charset

    # 脚本参数分析
    # 分析参数确定是否手动执行
    def arg_analyse(self):
        args = sys.argv
        if len(args) == 2:
            arg_key = args[1].split("=")[0]
            arg_value = args[1].split("=")[1]
            if arg_key == "use_crontab" and arg_value == "true":
                self.use_crontab = arg_value
            else:
                self.use_crontab = "false"
        else:
            self.use_crontab = "false"

    # base64解密
    def password_decode(self):
        if self.use_pwd_coding == "true":
            self.password = base64.b64decode(self.password_encoding.encode()).decode()
            print("password decode complete")

    def set_sqllist(self, sqllisted):
        try:
            self.sqlslist = sqllisted
        except Exception as e:
            print(e)

    def get_sqlliste(self):
        try:
            sqllist = self.sqlslist
        except Exception as e:
            print(e)

    def mysql_main(self):
        self.password_decode
        host = self.host
        user = self.user
        password = self.password
        database = self.database
        port = int(self.port)
        charset = self.charset

        # SQL语句列表
        sqlslist = self.sqlslist

        mysqler = myut.MysqlUtil(host=host,
                                 user=user,
                                 password=password,
                                 database=database,
                                 port=port,
                                 charset=charset,
                                 sqlslist=sqlslist)
        mysqler.cursor_cnn()

        # 调用方法插入数据
        mysqler.insertdatas()

    def loaddata_main(self):
        # dataloader = DATALOADER()
        if self.usemysql == "true":
            self.mysql_main()
        else:
            # 其它数据库入库表设计，与数据库入库工具待补充
            print("其他数据库方法，请联系研发人员补充数据入库相关的util工具")

# if __name__ == '__main__':
#     main

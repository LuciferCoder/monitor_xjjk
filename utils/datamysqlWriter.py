#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年7月18日10:38:11
# 通用类
# 功能： MySQL数据库写入
#

import json
from bin import dataLoad
import os
import sys

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# 此方法可以重构，提出到单独的类方法以精简代码量
"""
# 取所有指标参数，整合成为datajson,所有值不为空的dic的key作为关键字，值作为insert的值，生成sql语句，将语句写入本地文件、json数据写入本地文件
# 整合的sql语句列表调用返回给 dataLoad 作为参数set进去，执行insert
# 文件名： 年月日时分秒_组件名_ddl.json
# 文件名： 年月日时分秒_组件名_ddl.sql
"""


class DATAMYSQLWRITER(object):
    def __init__(self):
        self.dataload_hive_json_filenamePath = None
        self.dataload_hive_sql_filenamePath = None
        self.table_name = None
        self.dataloader = dataLoad.DATALOADER()

    def set_dataload_hive_json_filenamePath(self, set_dataload_hive_json_filenamePath):
        self.dataload_hive_json_filenamePath = set_dataload_hive_json_filenamePath

    def get_set_dataload_hive_json_filenamePath(self):
        return self.set_dataload_hive_json_filenamePath

    def set_dataload_hive_sql_filenamePath(self, dataload_hive_sql_filenamePath):
        self.dataload_hive_sql_filenamePath = dataload_hive_sql_filenamePath

    def get_dataload_hive_sql_filenamePath(self):
        return self.dataload_hive_sql_filenamePath

    def set_table_name(self, table_name):
        self.table_name = table_name

    def get_table_name(self):
        return self.table_name

    # 生成sql语句
    def datamysqlAllwriter(self):
        sqllist = []
        # 字典
        jsonfile = self.dataload_hive_json_filenamePath
        # "insert into table(key1, key2, key3) values(value1,value2,value3);"
        sqlfile = open(self.dataload_hive_sql_filenamePath, 'a', encoding="utf-8")
        table_name = self.table_name
        with open(jsonfile, 'r') as file:
            dic_list = file.readlines()
            for dic in dic_list:
                dic = json.loads(dic)
                keys = dic.keys()
                values = []
                for key in keys:
                    value = dic["%s" % key]
                    values.append(value)
                # 生成一条语句：
                key_string = ",".join(keys)
                values_string = "'" + "','".join(values) + "'"
                sql = "insert into %s(%s) values(%s);\n" % (table_name, key_string, values_string)
                sqllist.append(sql)
                # 写入sql语句到sql文件
                sqlfile.write(sql)
            file.close()
        sqlfile.close()

        self.dataloader.set_sqllist(sqllisted=sqllist)
        self.dataloader.loaddata_main()

        # 数据写入本地json文件
        # 在其他住区指标的地方，拼接指标dic串，调用此方法写入到本地文件中
        # 此方法可以重构，提出到单独的类方法以精简代码量

    # def jsondata_writer(self, dicstring):
    #     jsonfile = self.dataload_hive_json_filenamePath
    #     with open(jsonfile, 'a', encoding="utf-8") as file:
    #         file.write(dicstring + "\n")
    #         file.close()

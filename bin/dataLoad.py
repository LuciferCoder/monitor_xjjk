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

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# 数据库导入到MySQL
import utils.mysqlUtil as myut
import utils.hiveUtils as hvut

from utils import hdfsFileUtils as hdfsfiler


# 使用mysql作为数据导入
class DATALOADER(object):
    def __init__(self):
        self.BASE_DIR = BASE_DIR
        self.jsonfile_path = self.BASE_DIR + "/conf/dataload/dataload.json"
        name, version, usemysql, use_pwd_coding, user, port, host, password_encoding, \
        password, database, datalaod_hive_json, datalaod_hive_sql, datalaod_hdfs_json, \
        datalaod_hdfs_sql, datalaod_yarn_json, datalaod_yarn_sql, table_name, charset = self._json_parse()
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
                   datalaod_hdfs_sql, datalaod_yarn_json, datalaod_yarn_sql, table_name, charset

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


# hive时的配置文件dataloadhiver.json
class DATALOADHIVER(object):

    def __init__(self):
        self.client_keytab_principle = None
        self.name = None
        self.client_keytab = None
        self.krb5conf = None
        self.cmd = None
        self.csv_filepath = None
        self.dataloadhiver_conf_file = BASE_DIR + "/conf/dataload/dataloadhiver.json"
        self.name, \
        self.version, \
        self.use_hive, \
        self.use_pwd_coding, \
        self.user, \
        self.port, \
        self.host, \
        self.password_encoding, \
        self.password, \
        self.database, \
        self.datalaod_hive_json, \
        self.datalaod_hive_sql, \
        self.datalaod_hdfs_json, \
        self.datalaod_hdfs_sql, \
        self.datalaod_yarn_json, \
        self.datalaod_yarn_sql, \
        self.table_name, \
        self.charset, \
        self.use_kerberos = self.__parser__()

        # hiveserver2 IP地址、端口
        self.hiveserver2_ip = None
        self.hiveserver2_port = None

        self.hdfsclient = None

        # 传递csv

    def set_hdfsclient(self, hdfsclient):
        self.hdfsclient = hdfsclient

    def get_hdfsclient(self):
        return self.hdfsclient

    def set_cmd(self, cmd):
        self.cmd = cmd

    def get_cmd(self):
        return self.cmd

    # 设置和iveserver_ip
    def set_hiveserver2_ip(self, hiveserver2_ip):
        self.hiveserver2_ip = hiveserver2_ip

    # 设置和iveserver_port
    def get_hiveserver2_ip(self):
        return self.hiveserver2_ip

    # 设置和iveserver_port
    def set_hiveserver2_port(self, hiveserver2_port):
        self.hiveserver2_port = hiveserver2_port

    # 设置和iveserver_port
    def get_hiveserver2_port(self):
        return self.hiveserver2_port

    # 配置文件分析
    def __parser__(self):
        # print(self.jsonfile_path)
        with open(self.dataloadhiver_conf_file, 'r') as jsonfile:
            load_dict = json.load(jsonfile)

            name = load_dict["name"]
            version = load_dict["version"]

            # hive 配置
            usehive = load_dict["dependencies"]["config"]["use_hive"]
            use_kerberos = load_dict["dependencies"]["config"]["use_kerberos"]
            use_pwd_coding = load_dict["dependencies"]["hive"]["use_pwd_coding"]
            user = load_dict["dependencies"]["hive"]["user"]
            host = load_dict["dependencies"]["hive"]["host"]
            port = load_dict["dependencies"]["hive"]["port"]
            password_encoding = load_dict["dependencies"]["hive"]["password_encoding"]
            password = load_dict["dependencies"]["hive"]["password"]
            database = load_dict["dependencies"]["hive"]["database"]
            table_name = load_dict["dependencies"]["hive"]["table_name"]
            charset = load_dict["dependencies"]["hive"]["charset"]

            datalaod_hive_json = load_dict["dependencies"]["hive"]["dataload_hive"]["json_dic"]
            datalaod_hive_sql = load_dict["dependencies"]["hive"]["dataload_hive"]["sql"]
            datalaod_hdfs_json = load_dict["dependencies"]["hive"]["dataload_hdfs"]["json_dic"]
            datalaod_hdfs_sql = load_dict["dependencies"]["hive"]["dataload_hdfs"]["sql"]
            datalaod_yarn_json = load_dict["dependencies"]["hive"]["dataload_yarn"]["json_dic"]
            datalaod_yarn_sql = load_dict["dependencies"]["hive"]["dataload_yarn"]["sql"]

            return name, version, usehive, use_pwd_coding, user, port, host, password_encoding, \
                   password, database, datalaod_hive_json, datalaod_hive_sql, datalaod_hdfs_json, \
                   datalaod_hdfs_sql, datalaod_yarn_json, datalaod_yarn_sql, table_name, charset, use_kerberos

    def hiveut_main(self):
        auth = None
        # host, port, auth, database, kerberos_service_name, loaddata_sql)
        try:
            if self.use_kerberos == "true":
                auth = "KERBEROS"
        except Exception as e:
            print("hiveut_main,KERBEROS: ", e)

        cmd = self.get_cmd()
        hiveserver2_ip = self.get_hiveserver2_ip()
        hiveserver2_port = self.get_hiveserver2_port()
        print("dataLoad.py hiveserver2_ip: ", hiveserver2_ip)
        print("dataLoad.py hiveserver2_port: ", hiveserver2_port)

        hiver = hvut.HIVEUTILS(host=hiveserver2_ip,
                               port=hiveserver2_port,
                               auth=auth,
                               database=self.database,
                               kerberos_service_name="hive",
                               cmd=cmd)
        """
        传参数到hiveUtils,进行kerberos认证
        来源路线：hivePD --> datahivewriter --> dataLoad -->hiveUtils
        """
        krb5conf = self.get_krb5conf()
        hiver.set_krb5conf(krb5conf)

        client_keytab = self.get_client_keytab()
        hiver.set_client_keytab(client_keytab)

        name = self.get_cluster_name()
        hiver.set_cluster_name(name)

        client_keytab_principle = self.get_client_keytab_principle()
        hiver.set_client_keytab_principle(client_keytab_principle)

        hiver.cnn()
        hiver.cursor_self()

        # hdfser = hdfsfiler.HDFSFILETUTILS()

        # hdfs上传文件

        # hdfsclient = self.get_hdfsclient()
        hdfsclient = self.get_hdfsclient()
        # hdfs需要的用户初始化
        hdfsclient.krb5init()
        # # 创建目录
        # hdfser.hdfsmkdir()
        print("dataLoad: ", hdfsclient.get_hdfshost())
        hdfsclient.hdfsmkdir()
        # # 上传文件,
        # hdfser.hdfsput()
        hdfsclient.hdfsput()

        hiver.krb5init()

        hiver.exec_cmd()

        # hdfs 删除文件
        hdfsclient.krb5init()
        hdfsclient.hdfsdel()

        # cursor.execute('SELECT * FROM test_db.test_parquet')
        # print(cursor.fetchone())
        # print(cursor.fetchall())

    def loaddata_main(self):
        # dataloader = DATALOADER()
        if self.use_hive == "true":
            self.hiveut_main()
        else:
            # 其它数据库入库表设计，与数据库入库工具待补充
            print("其他数据库方法，请联系研发人员补充数据入库相关的util工具")

    def set_csv_filepath(self, csv_filepath):
        self.csv_filepath = csv_filepath

    def get_csv_filepath(self):
        return self.csv_filepath

    def get_krb5conf(self):
        return self.krb5conf

    def set_krb5conf(self, krb5conf):
        self.krb5conf = krb5conf

    def get_client_keytab(self):
        return self.client_keytab

    def set_client_keytab(self, client_keytab):
        self.client_keytab = client_keytab

    def set_cluster_name(self, name):
        self.name = name

    def get_cluster_name(self):
        return self.name

    def get_client_keytab_principle(self):
        return self.client_keytab_principle

    def set_client_keytab_principle(self, client_keytab_principle):
        self.client_keytab_principle = client_keytab_principle

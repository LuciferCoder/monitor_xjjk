#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年6月28日17:37:53
# 通用类
# 功能： 导入述文件到mysql数据库表
# 主要使用 pyhive

from pyhive import hive
import os
import sys
import krbticket

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)


# hive 数据导入
# 待补充
class HIVEUTILS(object):

    def __init__(self, host, port, auth, database, kerberos_service_name, cmd):
        self.name = None
        self.client_keytab_principle = None
        self.client_keytab = None
        self.krb5conf = None
        self.cursor = None
        self.conn = None
        self.host = host
        self.port = port
        self.auth = auth
        self.database = database
        self.kerberos_service_name = kerberos_service_name
        # self.cmd = None
        self.cmd = cmd

    def set_name(self, name):
        self.name = name

    def get_name(self):
        return self.name

    def set_client_keytab_principle(self, client_keytab_principle):
        self.client_keytab_principle = client_keytab_principle

    def get_client_keytab_principle(self):
        return self.client_keytab_principle

    def set_krb5conf(self, krb5conf):
        self.krb5conf = krb5conf

    def get_krb5conf(self):
        return self.krb5conf

    def set_client_keytab(self, krb5conf):
        self.krb5conf = krb5conf

    def get_client_keytab(self):
        return self.krb5conf

    def set_host(self, host):
        self.host = host

    def get_port(self):
        return self.port

    # 传入值
    def set_cmd(self, cmd):
        self.cmd = cmd

    # get 值
    def get_cmd(self):
        return self.cmd

    def krb5init(self):
        # krbconf = self.krb5conf
        krbconf = self.get_krb5conf()
        # keytab_file = self.client_keytab
        keytab_file = self.get_client_keytab()
        # principle = self.client_keytab_principle
        principle = self.get_client_keytab_principle()
        os.environ['KRB5CCNAME'] = os.path.join(BASE_DIR, f'keytab/krb5cc_%s' % (self.name))
        kconfig = krbticket.KrbConfig(principal=principle, keytab=keytab_file)
        krbticket.KrbCommand.kinit(kconfig)
        # cont = krbticket.KrbTicket.get(keytab=keytab_file,principal=principle)

    def cnn(self):
        # conn = hive.Connection(host="localhost",
        #                        port=10000,
        #                        auth="KERBEROS",
        #                        database="test_db",
        #                        kerberos_service_name="hive")

        print("host: ", self.host, "\nport: ", self.port, "\nauth: ", self.auth, "\ndatabase: ",
              self.database,
              "\nkerberos_service_name: ", self.kerberos_service_name,
              "\ncmd: ", self.cmd)
        # 认证Kerberos
        self.krb5init()
        # 连接
        conn = hive.Connection(host=self.host,
                               port=self.port,
                               auth=self.auth,
                               database=self.database,
                               kerberos_service_name=self.kerberos_service_name)

        self.conn = conn

    def cursor_self(self):
        conn = self.conn
        cursor = conn.cursor()
        self.cursor = cursor

    def exec_cmd(self):
        try:
            cursor = self.cursor
            cmd = self.cmd
            cursor.execute(cmd)
            result = cursor.fetchall()
            for re in result:
                print(re)
        except Exception as e:
            print("hiver.exec_cmd: ", e)

    def cursor_close(self):
        cursor = self.cursor
        cursor.close()

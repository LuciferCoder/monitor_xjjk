#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年6月28日17:37:53
# 通用类
# 功能： 导入述文件到mysql数据库表
# 主要使用 pyhive

from pyhive import hive


# hive 数据导入
# 待补充
class HIVEUTILS(object):

    def __init__(self, host, port, auth, database, kerberos_service_name, cmd):
        self.cursor = None
        self.conn = None
        self.host = host
        self.port = port
        self.auth = auth
        self.database = database
        self.kerberos_service_name = kerberos_service_name
        self.cmd = None
        self.cmd = cmd

    # 传入值
    def set_cmd(self, cmd):
        self.cmd = cmd

    # get 值
    def get_cmd(self):
        return self.cmd

    def cnn(self):
        # conn = hive.Connection(host="localhost",
        #                        port=10000,
        #                        auth="KERBEROS",
        #                        database="test_db",
        #                        kerberos_service_name="hive")

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

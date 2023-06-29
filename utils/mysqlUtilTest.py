#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年6月28日17:37:53
# 通用类
# 功能： 导入述文件到mysql数据库表
# 主要使用 pymysql


# mysql工具
import pymysql


# 使用python连接mysql数据库，并对数据库进行添加数据的方法
# 创建连接，数据库主机地址 数据库用户名称 密码 数据库名 数据库端口 数据库字符集编码


class MysqlUtil(object):

    def __init__(self, host, user, password, database, port, charset, sqlslist):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.charset = charset
        self.sqlslist = sqlslist
        self.cursor = None
        self.conn = None
        # cursor, conn = self.cursor_cnn()
        # self.cursor = cursor
        # self.conn = conn

    def cursor_cnn(self):
        try:
            conn = pymysql.connect(host=self.host,
                                   user=self.user,
                                   password=self.password,
                                   database=self.database,
                                   port=int(self.port),
                                   charset=self.charset)
            print("连接成功")
            # 创建游标
            cursor = conn.cursor()
            # return cursor, conn
            self.cursor=cursor
            self.conn = conn

        except Exception as e:
            print(e)

    # 批量添加数据
    def insertdatas(self):
        cursor = self.cursor
        conn = self.conn

        # 连续插入数据
        try:
            for sql in self.sqllist:
                cursor.execute(sql)
                conn.commit()
            print("insert complete...")
            self.closeconn()
        except Exception as e:
            print(e)
            conn.rollback()
            self.closeconn()

    # 关闭游标跟连接
    def closeconn(self):
        cursor = self.cursor
        conn = self.conn
        # 关闭游标
        cursor.close()
        # 关闭连接
        conn.close()



def main():
    # import pymysql
    host = "10.0.0.5"
    user = "root"
    port = 3306
    password = "0gvzJr66iNs5"
    database = "monitorxjjk"
    sqllist = ["insert into monitorxjjk(bigdata_component,component_service,date,time,component_service_status,ip,hostname) values('hive','hiveserver2','20230629','184700','0','10.0.0.42','ocean-bigdata-1a-22');"]

    mysqler = MysqlUtil(host,user,port,password,database,sqlslist=sqllist,charset="utf-8")




if __name__ == '__main__':
    main()
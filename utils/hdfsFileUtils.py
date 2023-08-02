#!/bin/env python3
# -*- coding: utf-8 -*-


import os
import sys
import krbticket
from hdfs.ext.kerberos import KerberosClient
import requests

# import logging

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

"""
工具主要目的是上传文件到hdfs目录，dataload完成之后删除文件
"""


class HDFSFILETUTILS(object):

    # 初始化参数
    def __init__(self):
        # 文件路径参数
        # hdfs,此处应该是 hdfs
        self.hdfsport = None
        # hdfs端口
        self.hdfshost = None
        self.bigdata_comment_name = "HDFSFILETUTILS"
        # 此处应该是 hdfs/ocean
        self.client_keytab_principle = None
        # /export/kerberos/1/hdfs.keytab
        self.client_keytab = None
        # /etc/krb5.conf
        self.krb5conf = None
        self.csv_filepath = None
        self.hdfs_base_dir = "/tmp"
        self.date_date = None
        # hdfs上传文件路径：/tmp/${date}/${filename}
        self.hdfs_client = None

    def get_hdfs_base_dir(self):
        return self.hdfs_base_dir

    def set_krb5conf(self, krb5conf):
        self.krb5conf = krb5conf

    def get_krb5conf(self):
        return self.krb5conf

    def set_client_keytab(self, client_keytab):
        self.client_keytab = client_keytab

    def get_client_keytab(self):
        return self.client_keytab

    def set_client_keytab_principle(self, client_keytab_principle):
        self.client_keytab_principle = client_keytab_principle

    def get_client_keytab_principle(self):
        return self.client_keytab_principle

    # hdfs client
    def set_hdfs_client(self):
        # client = HdfsClient(hosts='localhost:50070')
        # logging.basicConfig(level=logging.DEBUG)

        """
        测试成功
        """

        session = requests.Session()
        session.verify = False

        host = self.get_hdfshost()
        port = self.get_hdfsport()
        principle = self.get_client_keytab_principle()

        client = KerberosClient(url='https://%s:%s' % (host, port),
                                session=session,
                                mutual_auth='REQUIRED',
                                principal='%s' % principle)
        self.hdfs_client = client

    # 返回 hdfs client
    def get_hdfs_client(self):
        return self.hdfs_client

    # 日期参数传入
    def set_date(self, date):
        self.date_date = date

    # 日期参数返回
    def get_date_date(self):
        return self.date_date

    # 返回csv本地文件参数
    def get_csv_filepath(self):
        return self.csv_filepath

    # 传入参数，传入参数后调用参数必须使用返回参数函数进行调用
    def set_csv_filepath(self, csv_filepath):
        self.csv_filepath = csv_filepath

    # 文件按上传
    def hdfsput(self):
        client = self.get_hdfs_client()
        datestr = str(self.get_date_date())[0:8]
        hdfs_base_dir = self.hdfs_base_dir
        dest_dir = "/".join([hdfs_base_dir, datestr])
        curfilepath = self.get_csv_filepath()
        print("hdfs utils curfilepath: ", curfilepath)
        print("hdfs utils hdfs_path: ", dest_dir)
        client.upload(hdfs_path=dest_dir, local_path=curfilepath)

    # 文件下载，目前用不到，暂时不编辑
    def hfdsget(self):
        pass

    # hdfs文件删除
    def hdfsdel(self):
        client = self.get_hdfs_client()
        # client = HdfsClient(hosts="localhost:50070")
        datestr = str(self.get_date_date())[0:8]
        hdfs_base_dir = self.hdfs_base_dir
        dest_dir = "/".join([hdfs_base_dir, datestr])
        curfilepath = self.get_csv_filepath()
        filename = os.path.basename(curfilepath)
        path = dest_dir + "/" + filename
        client.delete(hdfs_path=path)

    # 创建文件夹目录
    def hdfsmkdir(self):
        client = self.get_hdfs_client()
        # client = HdfsClient(hosts="localhost:50070")
        datestr = str(self.get_date_date())[0:8]
        hdfs_base_dir = self.hdfs_base_dir
        dest_dir = "/".join([hdfs_base_dir, datestr])
        # curfilepath = self.get_csv_filepath()
        # filename = os.path.basename(curfilepath)
        # path = dest_dir + "/" + filename
        # client.mkdirs(dest_dir)
        client.makedirs(hdfs_path=dest_dir, permission='755')

    # kerberos认证
    # 如果使用了kerberos，调用此方法
    def krb5init(self):
        krbconf = self.get_krb5conf()
        # keytab_file = self.client_keytab
        keytab_file = self.get_client_keytab()
        # principle = self.client_keytab_principle
        principle = self.get_client_keytab_principle()
        os.environ['KRB5CCNAME'] = os.path.join(BASE_DIR, f'keytab/krb5cc_%s' % self.bigdata_comment_name)
        kconfig = krbticket.KrbConfig(principal=principle, keytab=keytab_file)
        krbticket.KrbCommand.kinit(kconfig)
        # cont = krbticket.KrbTicket.get(keytab=keytab_file,principal=principle)

    def get_hdfshost(self):
        return self.hdfshost

    def set_hdfshost(self, hdfs_host):
        self.hdfshost = hdfs_host

    def get_hdfsport(self):
        return self.hdfsport

    def set_hdfsport(self, hdfsport):
        self.hdfsport = hdfsport
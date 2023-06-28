#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年6月28日17:37:53
# 通用类
# 功能： 导入述文件到hive数据库表
# 主要使用 pyhive

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


class DATALOADER():
    def __init__(self):
        self.BASE_DIR = BASE_DIR
        self.jsonfile_path = self.BASE_DIR + "/conf/dataload/dataload.json"
        name, version, cluster_name, hiveconf, krb5conf, client_keytab, \
            client_keytab_principle, use_kerberos, ssh_user, ssh_pkey, \
            hiveserver2_node_list, hiveserver2_node_port, \
            metastore_node_list, metastore_node_port = self._json_parse()
        self.name = name
        self.version = version
        self.cluster_name = cluster_name
        self.krb5conf = krb5conf
        self.client_keytab = client_keytab
        self.client_keytab_principle = client_keytab_principle
        self.hiveconfname = hiveconf
        self.hiveserver2_node_list = hiveserver2_node_list
        self.hiveserver2_node_port = hiveserver2_node_port
        self.metastore_node_list = metastore_node_list
        self.metastore_node_port = metastore_node_port

        # self.hiveconf = hiveconf

        self.hiveconf_path = os.path.dirname(self.jsonfile_path) + "/" + self.hiveconfname
        # hdfs_nodes数量，来自于健康检查返回的节点数
        self.hdfs_node_count = 0
        self.use_kerberos = use_kerberos

        self.hive_site_file = "/conf/%s/%s" % (self.name, self.hiveconfname)

        self.hive_site_filepath = self.BASE_DIR + self.hive_site_file

        # 判断脚本执行参数,默认值 false 按照手动执行打印宕机的datanode主机;
        # 值为true的按照定时任务执行，不打印宕机的节点，只抓取指标写入表中;
        # 其它值无效
        self.use_crontab = "false"
        # self.use_crontab = "true"
        # 当前时间： 年月日时分
        self.datenow = datetime.now()
        self.datenowstring = self.datenow.strftime("%Y%m%d%H%M%S")
        # 前一天的当前时间
        self.lastdayofnow = self.datenow - timedelta(days=1)
        self.lastdayofnowstring = self.lastdayofnow.strftime("%Y%m%d%H%M%S")
        # 前一天的年月日
        self.lastdayofdate = self.lastdayofnow.strftime("%Y%m%d")

        self.curday_cap = ""
        self.ssh_user = ssh_user
        self.ssh_pkey = ssh_pkey

        self.hivesevrer2_node_isalived_list = []
        self.metastore_node_isalived_list = []

        self.hiveserverNode_jmx_result = []
        self.metastoreNode_jmx_result = []

        self.threadnum_list = []

    # hive配置文件参数分析
    def _json_parse(self):
        # print(self.jsonfile_path)
        with open(self.jsonfile_path, 'r') as jsonfile:
            load_dict = json.load(jsonfile)

            name = load_dict["name"]
            version = load_dict["version"]

            # 集群名称
            cluster_name = load_dict["dependencies"]["config"]["cluster_name"]

            # hive-site.xml
            hiveconf = load_dict["dependencies"]["config"]["hivesconf"]

            # 集群是否使用了kerberos
            use_kerberos = load_dict["dependencies"]["config"]["use_kerberos"]

            # Kerberos相关配置
            krb5conf = load_dict["dependencies"]["kerberos"]["krb5conf"]
            client_keytab = load_dict["dependencies"]["kerberos"]["keytab"]
            client_keytab_principle = load_dict["dependencies"]["kerberos"]["client_principle"]

            # 集群节点信息
            # 赶工期，目前支持key部署，密钥免密
            ssh_user = load_dict["dependencies"]["config"]["ssh_user"]
            ssh_pkey = load_dict["dependencies"]["config"]["ssh_pkey"]

            ##hiveserver2 node list
            hiveserver2_node_list = load_dict["dependencies"]["hivenodes"]["hiveserver2"]["hiveserverNodes"]
            hiveserver2_node_port = load_dict["dependencies"]["hivenodes"]["hiveserver2"]["hiveserverPort"]

            ##hive metastore node list
            metastore_node_list = load_dict["dependencies"]["hivenodes"]["metastore"]["metastoreNodes"]
            metastore_node_port = load_dict["dependencies"]["hivenodes"]["metastore"]["metastorePort"]

            return name, version, cluster_name, hiveconf, krb5conf, client_keytab, client_keytab_principle, \
                use_kerberos, ssh_user, ssh_pkey, hiveserver2_node_list, hiveserver2_node_port, \
                metastore_node_list, metastore_node_port

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

    # kerberos 认证
    # 认证krb5
    # 如果使用了kerberos，调用此方法
    def krb5init(self):
        krbconf = self.krb5conf
        keytab_file = self.client_keytab
        principle = self.client_keytab_principle
        os.environ['KRB5CCNAME'] = os.path.join(self.BASE_DIR, f'keytab/krb5cc_%s' % (self.name))
        kconfig = krbticket.KrbConfig(principal=principle, keytab=keytab_file)
        krbticket.KrbCommand.kinit(kconfig)
        # cont = krbticket.KrbTicket.get(keytab=keytab_file,principal=principle)


import pyhive

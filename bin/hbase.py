#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年6月25日19:58:38

import warnings

# 屏蔽本条语句之后所有的告警
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


class HBASER(object):

    def __init__(self):
        pass

    # hive配置文件参数分析
    def _json_parse(self):
        with open(self.jsonfile_path, 'r') as jsonfile:
            load_dict = json.load(jsonfile)

            name = load_dict["name"]
            version = load_dict["version"]

            # 集群名称
            cluster_name = load_dict["dependencies"]["config"]["cluster_name"]

            # hdfs-site.xml
            hdfsconf = load_dict["dependencies"]["config"]["hdfsconf"]

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

            ##rm
            rm1 = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm1"]
            rm1_port = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm1_port"]
            rm2 = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm2"]
            rm2_port = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm2_port"]

            ##nodemanager
            # dic list
            nodemanager_list = load_dict["dependencies"]["hadoop_nodes"]["nodemanager"]

            # nodemanager_port
            nodemanagerJmxport = load_dict["dependencies"]["hadoop_nodes"]["nodemanagerJmxport"]

            return name, version, cluster_name, hdfsconf, krb5conf, client_keytab, client_keytab_principle, rm1, rm2, rm1_port, rm2_port, nodemanager_list, use_kerberos, ssh_user, ssh_pkey, nodemanagerJmxport


def main_one():
    pass


if __name__ == '__main__':
    main_one()

#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年7月5日10:34:11

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

# 数据导入到mysql
import dataLoad

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)


class SERVICES(object):
    # 初始化参数
    def __init__(self):
        pass


    def configer_parser(self):
        pass

def main():
    pass


if __name__ == '__main__':
    main()

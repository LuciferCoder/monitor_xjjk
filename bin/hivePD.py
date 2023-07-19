#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年6月25日19:58:38
# 要求 hiveserver开启web ui，如果漏洞原因关闭了 hive的web ui则使用脚本 hivePD.py进行指标抓取

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


class HIVER(object):
    def __init__(self):
        self.BASE_DIR = BASE_DIR
        self.jsonfile_path = self.BASE_DIR + "/conf/hivePD/hive.json"
        name, version, cluster_name, hiveconf, krb5conf, client_keytab, \
            client_keytab_principle, use_kerberos, ssh_user, ssh_pkey, \
            hiveserver2_node_list, hiveserver2_node_port, \
            metastore_node_list, metastore_node_port, dataload_type, dataload_time = self._json_parse()
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
        self.dataload_type = dataload_type
        self.dataload_time = dataload_time

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
        # 当前时间： 年月日时分秒
        self.datenow = datetime.now()
        self.datenowstring = self.datenow.strftime("%Y%m%d%H%M%S")
        self.datenowdate = self.datenow.strftime("%Y%m%d")
        self.datenowtime = self.datenow.strftime("%H%M%S")
        # 前一天的当前时间
        self.lastdayofnow = self.datenow - timedelta(days=1)
        self.lastdayofnowstring = self.lastdayofnow.strftime("%Y%m%d%H%M%S")
        # 前一天的年月日
        self.lastdayofdate = self.lastdayofnow.strftime("%Y%m%d")
        self.datenowdate = self.datenow.strftime("%Y%m%d")

        self.curday_cap = ""
        self.ssh_user = ssh_user
        self.ssh_pkey = ssh_pkey

        self.hivesevrer2_node_isalived_list = []
        self.metastore_node_isalived_list = []

        self.hiveserverNode_jmx_result = []
        self.metastoreNode_jmx_result = []

        # 数据写入本地，并导入mysql
        self.threadnum_list = []
        # 初始化dataload对象
        self.dataloader = dataLoad.DATALOADER()

        self.dataload_hive_json_dir = self.dataloader.datalaod_hive_json
        self.dataload_hive_json_dir_abs = BASE_DIR + self.dataload_hive_json_dir
        self.dataload_hive_json_filenamePath = self.dataload_hive_json_dir_abs + "/%s_hive_dml.json" % self.datenowstring

        self.dataload_hive_json_dir = self.dataloader.datalaod_hive_sql
        self.dataload_hive_sql_dir_abs = BASE_DIR + self.dataload_hive_json_dir
        self.dataload_hive_sql_filenamePath = self.dataload_hive_sql_dir_abs + "/%s_hive_dml.sql" % self.datenowstring

        self.table_name = self.dataloader.table_name
        # 指标值字典列表，用于dataLoad生成语句

        # self.json_writer = None

    #
    # def set_json_writer(self, json_writer):
    #     self.json_writer = json_writer

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

            # 数据导入类型
            dataload_type = load_dict["dependencies"]["config"]["dataload_type"]
            dataload_time = load_dict["dependencies"]["config"]["dataload_time"]

            return name, version, cluster_name, hiveconf, krb5conf, client_keytab, client_keytab_principle, \
                use_kerberos, ssh_user, ssh_pkey, hiveserver2_node_list, hiveserver2_node_port, \
                metastore_node_list, metastore_node_port, dataload_type, dataload_time

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

    # 端口连通性检查,端口探活使用
    def socket_check(self, ip, port):
        # print("调用函数 socket_check 成功")
        try:
            s = socket.socket()
            # 设置超时5s,超时返回字符false
            s.settimeout(5)
            s.connect((ip, int(port)))
            s.close()
            return "true"
        except Exception as e:
            print(e)
            return "false"

    # 所有节点连通性检查，端口探活
    def IsAlivedNode(self):
        print("调用 isalived 成功")
        # hiveserver2
        hivesevrer2_node_isalived_list = []
        hiverserver2_port = self.hiveserver2_node_port
        hiveserver2nodeslist = self.hiveserver2_node_list
        for hiverservernode in hiveserver2nodeslist:
            hivenode_json = hiverservernode
            ip = hivenode_json["ip"]
            hostname = hivenode_json["hostname"]
            node_is_alived = self.socket_check(ip=ip, port=hiverserver2_port)
            jsonstr = '{"ip":"%s","hostname":"%s","alived":"%s"}' % (ip, hostname, node_is_alived)
            hivesevrer2_node_isalived_list.append(jsonstr)

        # metastore
        metastore_node_isalived_list = []
        metastore_port = self.metastore_node_port
        hive_metastore_list = self.metastore_node_list
        for hivemetastore in hive_metastore_list:
            hivemetastore_json = hivemetastore
            ip = hivemetastore_json["ip"]
            hostname = hivemetastore_json["hostname"]
            node_is_alived = self.socket_check(ip=ip, port=metastore_port)
            jsonstr = '{"ip":"%s","hostname":"%s","alived":"%s"}' % (ip, hostname, node_is_alived)
            metastore_node_isalived_list.append(jsonstr)

        self.hivesevrer2_node_isalived_list = hivesevrer2_node_isalived_list
        self.metastore_node_isalived_list = metastore_node_isalived_list

        # 调用服务打印
        self.screeOownServices()

    # 打印down的服务
    # 指标：服务端口探活
    def screeOownServices(self):
        print("调用打印服务")
        hivesevrer2_node_isalived_list = self.hivesevrer2_node_isalived_list
        metastore_node_isalived_list = self.metastore_node_isalived_list

        # print("hivesevrer2_node_isalived_list")
        # print(hivesevrer2_node_isalived_list)
        # print("metastore_node_isalived_list")
        # print(metastore_node_isalived_list)

        # 数据写入字段：
        # 服务：bigdata_component
        # 角色： component_service
        # 日期：date
        # 时间：time
        # 存活状态：component_service_status
        # IP: ip
        # 主机名：hostname

        bigdata_component = self.name
        date = self.datenowdate
        time = self.datenowtime

        for node in hivesevrer2_node_isalived_list:
            component_service = "hiveserver2"

            node_json = json.loads(node)
            isalived = node_json["alived"]
            if isalived == "true":
                # 存活为0
                component_service_status = 0
                ip = node_json["ip"]
                hostname = node_json["hostname"]
                dic_string = '{"bigdata_component":"%s","component_service":"%s","date":"%s","time":"%s","component_service_status":"%s","ip":"%s","hostname":"%s"}' % \
                             (bigdata_component, component_service, date, time, component_service_status, ip, hostname)
                self.jsondata_writer(dic_string)
            else:
                # 不存活为1
                component_service_status = 1
                ip = node_json["ip"]
                hostname = node_json["hostname"]
                dic_string = '{"bigdata_component":"%s","component_service":"%s","date":"%s","time":"%s","component_service_status":"%s","ip":"%s","hostname":"%s"}' % \
                             (bigdata_component, component_service, date, time, component_service_status, ip, hostname)
                self.jsondata_writer(dic_string)
                print("主机 %s, IP %s 上的服务 HiveServer2 服务 Down" % (hostname, ip))

        # 数据写入字段：
        # 服务：hive
        # 角色： metastore
        # 日期：date
        # 时间：time
        # 存活状态：component_service_status
        # IP: ip
        # 主机名：hostname

        for node in metastore_node_isalived_list:
            component_service = "metastore"
            node_json = json.loads(node)
            isalived = node_json["alived"]
            if isalived == "true":
                # 存活为0
                component_service_status = 0
                ip = node_json["ip"]
                hostname = node_json["hostname"]
                dic_string = '{"bigdata_component":"%s","component_service":"%s","date":"%s","time":"%s","component_service_status":"%s","ip":"%s","hostname":"%s"}' % \
                             (bigdata_component, component_service, date, time, component_service_status, ip, hostname)
                self.jsondata_writer(dic_string)
            else:
                ip = node_json["ip"]
                hostname = node_json["hostname"]
                # 不存活为1
                component_service_status = 1
                dic_string = '{"bigdata_component":"%s","component_service":"%s","date":"%s","time":"%s","component_service_status":"%s","ip":"%s","hostname":"%s"}' % \
                             (bigdata_component, component_service, date, time, component_service_status, ip, hostname)
                self.jsondata_writer(dic_string)
                print("主机 %s, IP %s 上的服务 Hive Metastore 服务 Down" % (hostname, ip))

    # 包装远程连接方法，执行命令返回结果
    def ssh_connect(self, ip, port, user, password, use_pwd, ssh_keyfile, cmd):
        # ssh获取远端服务进程信息
        ip = ip
        port = port
        user = user
        pwd = "password"
        ssh_key = ssh_keyfile
        use_pwd = use_pwd
        cmd = cmd

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_key = paramiko.RSAKey.from_private_key_file(ssh_key)

        if use_pwd == "true":
            pwd = password
            ssh.connect(ip, port, user, password, timeout=10)
            stdin, stdout, stderr = ssh.exec_command(cmd)
            # 输出命令执行结果
            result = stdout.read()
            ssh.close()
            return result

        else:
            # 使用密钥连接
            ssh.connect(hostname=ip, port=port, username=user, pkey=ssh_key)
            stdin, stdout, stderr = ssh.exec_command(cmd)
            # 输出命令执行结果
            result = stdout.read()
            ssh.close()
            return result

    # 获取hiveserver2jmx
    def hivejmx(self):
        # url = "http://%:%s/mx?qry=metrics:*" % (ip, port)

        hivesevrer2_node_isalived_list = self.hivesevrer2_node_isalived_list
        metastore_node_isalived_list = self.metastore_node_isalived_list
        hivesevrer2_port = self.hiveserver2_node_port

        hiveserverNode_jmx_result = []
        metastoreNode_jmx_result = []

        # 连接数
        threadnum_list = []

        # dataload写入字符串
        # 数据写入字段：
        # 服务：bigdata_component
        # 角色： component_service
        # 日期：date
        # 时间：time
        # GC时间：gctime
        # 堆内存使用率：heap_usage
        # IP: ip
        # 主机名：hostname
        # Hvieserver2的连接客户端数量：client_num

        # hiveserver2
        for hiveserver_node in hivesevrer2_node_isalived_list:
            node_json = json.loads(hiveserver_node)
            ip = node_json["ip"]
            hostname = node_json["hostname"]
            gctimevalue = ""
            heapusage = ""

            port = 22
            user = self.ssh_user
            password = ""
            use_pwd = "false"
            ssh_keyfile = self.ssh_pkey

            cmd = "ps -ef |grep org.apache.hive.service.server.HiveServer2|grep -v grep|awk '{print $2}'"

            service_pid = self.ssh_connect(ip, port, user, password, use_pwd, ssh_keyfile, cmd)
            service_pid = service_pid.decode()

            cmd = "source /etc/profile;sudo ${JAVA_HOME}/bin/jmap -heap %s" % service_pid
            heap_cont = self.ssh_connect(ip, port, user, password, use_pwd, ssh_keyfile, cmd)
            heap_cont = heap_cont.decode()

            # print("heap_cont")
            # print(heap_cont)
            usedlist = []
            MaxHeapSize = ""
            heap_cont_list = str(heap_cont).split("\n")
            # print("heap_cont_list")
            # print(heap_cont_list)
            for st in heap_cont_list:
                if "MaxHeapSize" in st:
                    MaxHeapSize = st.strip().split()[2]
                if "used" in st:
                    stlist = st.strip().split()
                    if len(stlist) == 4:
                        used = stlist[2]
                        usedlist.append(used)
            # print("MaxHeapSize")
            # print(MaxHeapSize)
            # print(type(MaxHeapSize))

            # 计算 heapuse
            used = 0
            for use in usedlist:
                used += float(use)
            # 使用率 "{:.2%}".format(curday_cap / 100).replace("%", "GB")
            usage = '{:.2%}'.format(float(used) / float(int(MaxHeapSize)))
            heapusage = usage

            # gc time
            cmd = " source /etc/profile;jstat -gcutil %s 5000 1" % service_pid
            gc_cont = self.ssh_connect(ip, port, user, password, use_pwd, ssh_keyfile, cmd)
            # gc_time = int(.strip().split()[-2])
            gc_contl = str(gc_cont).strip().split("\\n")[1]
            gc_time = gc_contl.strip().split()[-2]

            # 计算
            gctime = float(gc_time) * 1000
            gctimevalue = gctime

            # 组合字符串
            json_str = '{"hostname":"%s", "ip":"%s", "gctimevalue":"%s", "heapusage":"%s"}' \
                       % (hostname, ip, gctimevalue, heapusage)

            # hiveserverNode_jmx_result.append(json_str)

            # dataload写入字符串
            # 数据写入字段：
            # 服务：bigdata_component
            # 角色： component_service
            # 日期：date
            # 时间：time
            # GC时间：gctime
            # 堆内存使用率：heap_usage
            # IP: ip
            # 主机名：hostname
            # Hvieserver2的连接客户端数量：client_num

            heapusage = heapusage
            # heapusage = '{:.2%}'.format(float(heapusage))
            # heapusage = '{:.2%}'.format(heapusage)
            bigdata_component = self.name
            component_service = "hiveserver2"
            date = self.datenowdate
            time = self.datenowtime
            gctime = gctimevalue
            heap_usage = heapusage
            ip = ip
            hostname = hostname
            client_num = 0

            cmdwcclient = "netstat -lantup |grep 10000|grep ESTABLISHED|wc -l"
            client_num_encode = self.ssh_connect(ip, port, user, password, use_pwd, ssh_keyfile, cmdwcclient)
            client_num = client_num_encode.decode().strip()

            json_string = '{"bigdata_component":"%s","component_service":"%s","date":"%s","time":"%s","gctime":"%s","heap_usage":"%s","ip":"%s","hostname":"%s","client_num":"%s"}' % \
                          (bigdata_component, component_service, date, time, gctime, heap_usage, ip, hostname,
                           client_num)
            self.jsondata_writer(json_string)

            # heapusage = '{:.2%}'.format(heapusage)
            json_str = '{"hostname":"%s", "ip":"%s", "gctimevalue":"%s", "heapusage":"%s"}' \
                       % (hostname, ip, gctimevalue, heapusage)

            hiveserverNode_jmx_result.append(json_str)
            # usage = '{:.2%}'.format(float(used) / float(int(MaxHeapSize)))

            threadnum = client_num

            # 客户端连接数
            json_nu_str = '{"hostname":"%s", "ip":"%s", "threadnum":"%s"}' \
                          % (hostname, ip, threadnum)
            threadnum_list.append(json_nu_str)

        # metastore
        for metastore_node in metastore_node_isalived_list:
            node_json = json.loads(metastore_node)
            ip = node_json["ip"]
            hostname = node_json["hostname"]
            gctimevalue = ""
            heapusage = ""

            port = 22
            user = self.ssh_user
            password = ""
            use_pwd = "false"
            ssh_keyfile = self.ssh_pkey

            cmd = "ps -ef |grep hive.metastore.HiveMetaStore|grep -v grep|awk '{print $2}'"

            service_pid = self.ssh_connect(ip, port, user, password, use_pwd, ssh_keyfile, cmd)
            service_pid = service_pid.decode()

            cmd = "source /etc/profile;sudo ${JAVA_HOME}/bin/jmap -heap %s" % service_pid
            heap_cont = self.ssh_connect(ip, port, user, password, use_pwd, ssh_keyfile, cmd)
            heap_cont = heap_cont.decode()

            # print("heap_cont")
            # print(heap_cont)
            usedlist = []
            MaxHeapSize = ""
            heap_cont_list = str(heap_cont).split("\n")
            # print("heap_cont_list")
            # print(heap_cont_list)
            for st in heap_cont_list:
                if "MaxHeapSize" in st:
                    MaxHeapSize = st.strip().split()[2]
                if "used" in st:
                    stlist = st.strip().split()
                    if len(stlist) == 4:
                        used = stlist[2]
                        usedlist.append(used)
            # print("MaxHeapSize")
            # print(MaxHeapSize)
            # print(type(MaxHeapSize))

            # 计算
            used = 0
            for use in usedlist:
                used += float(use)
            # 使用率 "{:.2%}".format(curday_cap / 100).replace("%", "GB")
            usage = '{:.2%}'.format(float(used) / float(int(MaxHeapSize)))
            heapusage = usage

            # gc time
            cmd = " source /etc/profile;sudo ${JAVA_HOME}/bin/jstat -gcutil %s 5000 1" % service_pid
            gc_cont = self.ssh_connect(ip, port, user, password, use_pwd, ssh_keyfile, cmd)
            # gc_time = int(.strip().split()[-2])
            gc_contl = str(gc_cont).strip().split("\\n")[1]
            gc_time = gc_contl.strip().split()[-2]

            # 计算
            gctime = float(gc_time) * 1000
            gctimevalue = gctime

            # 组合字符串
            json_str = '{"hostname":"%s", "ip":"%s", "gctimevalue":"%s", "heapusage":"%s"}' \
                       % (hostname, ip, gctimevalue, heapusage)

            metastoreNode_jmx_result.append(json_str)

            # dataload写入字符串
            # 数据写入字段：
            # 服务：bigdata_component
            # 角色： component_service
            # 日期：date
            # 时间：time
            # GC时间：gctime
            # 堆内存使用率：heap_usage
            # IP: ip
            # 主机名：hostname
            # Hvieserver2的连接客户端数量：client_num
            bigdata_component = self.name
            component_service = "metastore"
            date = self.datenowdate
            time = self.datenowtime
            gctime = gctimevalue
            heap_usage = heapusage
            ip = ip
            hostname = hostname
            client_num = threadnum
            json_string = '{"bigdata_component":"%s","component_service":"%s","date":"%s","time":"%s","gctime":"%s","heap_usage":"%s","ip":"%s","hostname":"%s","client_num":"%s"}' % \
                          (bigdata_component, component_service, date, time, gctime, heap_usage, ip, hostname,
                           client_num)
            self.jsondata_writer(json_string)

        self.hiveserverNode_jmx_result = hiveserverNode_jmx_result
        self.metastoreNode_jmx_result = metastoreNode_jmx_result
        self.threadnum_list = threadnum_list

        # 调用函数
        self.screnn_hiveserver2_jmx_re()
        self.screnn_metastore_jmx_re()

    # screen hiveserver2 jmx result
    def screnn_hiveserver2_jmx_re(self):
        # '{"hostname":"%s", "ip":"%s", "gctimevalue":"%s", "heapusage":"%s"}'
        hiveserverNode_jmx_result = self.hiveserverNode_jmx_result
        for node in hiveserverNode_jmx_result:
            node_json = json.loads(node)
            hostname = node_json["hostname"]
            ip = node_json["ip"]
            gctimevalue = node_json["gctimevalue"]
            heapusage = node_json["heapusage"]
            print("主机 %s, IP %s 上的 HievServer2 服务的 GC时间：%s, 内存使用率：%s" % (
                hostname, ip, gctimevalue, heapusage))

    # screen hiveserver2 jmx result
    def screnn_metastore_jmx_re(self):
        # '{"hostname":"%s", "ip":"%s", "gctimevalue":"%s", "heapusage":"%s"}'
        metastoreNode_jmx_result = self.metastoreNode_jmx_result
        for node in metastoreNode_jmx_result:
            node_json = json.loads(node)
            hostname = node_json["hostname"]
            ip = node_json["ip"]
            gctimevalue = node_json["gctimevalue"]
            heapusage = node_json["heapusage"]
            print("主机 %s, IP %s 上的HievMetastore服务的GC时间：%s, 内存使用率：%s" % (
                hostname, ip, gctimevalue, heapusage))

    # 指标： Hvieserver2的连接客户端数量
    def hiveserver_client_num(self):
        threadnum_list = self.threadnum_list
        # {"hostname":"ocean-bigdata-1a-21", "ip":"10.0.0.47", "threadnum":"26"}
        # metastoreNode_jmx_result = self.metastoreNode_jmx_result
        for node in threadnum_list:
            # print(node)
            node_json = json.loads(node)
            hostname = node_json["hostname"]
            ip = node_json["ip"]
            threadnum = node_json["threadnum"]

            print("主机 %s, IP %s 上的HievServer2服务的客户端连接数：%s" % (hostname, ip, threadnum))

    """
    # 取所有指标参数，整合成为datajson,所有值不为空的dic的key作为关键字，值作为insert的值，生成sql语句，将语句写入本地文件、json数据写入本地文件
    # 整合的sql语句列表调用返回给 dataLoad 作为参数set进去，执行insert
    # 文件名： 年月日时分秒_组件名_ddl.json
    # 文件名： 年月日时分秒_组件名_ddl.sql
    """

    # 此方法可以重构，提出到单独的类方法以精简代码量
    # def datamysqlAllwriter(self):
    #     sqllist = []
    #     # 字典
    #     jsonfile = self.dataload_hive_json_filenamePath
    #     # "insert into table(key1, key2, key3) values(value1,value2,value3);"
    #     sqlfile = open(self.dataload_hive_sql_filenamePath, 'a', encoding="utf-8")
    #     table_name = self.table_name
    #     with open(jsonfile, 'r') as file:
    #         dic_list = file.readlines()
    #         for dic in dic_list:
    #             dic = json.loads(dic)
    #             keys = dic.keys()
    #             values = []
    #             for key in keys:
    #                 value = dic["%s" % key]
    #                 values.append(value)
    #             # 生成一条语句：
    #             key_string = ",".join(keys)
    #             values_string = "'" + "','".join(values) + "'"
    #             sql = "insert into %s(%s) values(%s);\n" % (table_name, key_string, values_string)
    #             sqllist.append(sql)
    #             # 写入sql语句到sql文件
    #             sqlfile.write(sql)
    #         file.close()
    #     sqlfile.close()
    #
    #     self.dataloader.set_sqllist(sqllisted=sqllist)
    #     self.dataloader.loaddata_main()

    # 数据写入本地json文件
    # 在其他住区指标的地方，拼接指标dic串，调用此方法写入到本地文件中
    # 此方法可以重构，提出到单独的类方法以精简代码量

    def jsondata_writer(self, dicstring):
        jsonfile = self.dataload_hive_json_filenamePath
        with open(jsonfile, 'a', encoding="utf-8") as file:
            file.write(dicstring + "\n")
            file.close()

    # # 重组hive导入文件格式，写入文件到csv文件，之后执行导入
    # def datahiveAllwriter(self):
    #     pass
    #


"""
主函数逻辑

指标需求：
四、hive
（1）组件服务的状态（大数据端口探活告警）

（2）Hive metastore的GC时间
（3）Hive metastore的内存使用

（4）Hive server2的内存使用
（5）Hiveserver2的GC时间
（6）Hvieserver2的连接客户端数量
"""


def main_one():
    hiver = HIVER()
    # exit(0)

    hiver.arg_analyse()

    if hiver.use_kerberos == "true":
        hiver.krb5init()

    # 组件状态（大数据端口探活警告）
    print("is alived start")
    hiver.IsAlivedNode()

    hiver.hivejmx()

    hiver.hiveserver_client_num()

    # 数据导入到mysql
    # 数据导入到mysql时启动以下数据导入：
    if hiver.dataload_type == "mysql":
        # hiver.datamysqlAllwriter()
        from utils import datamysqlWriter
        datamysqlWriter = datamysqlWriter.DATAMYSQLWRITER()

        datamysqlWriter.set_dataload_hive_json_filenamePath(hiver.dataload_hive_json_filenamePath)
        datamysqlWriter.set_table_name(hiver.table_name)
        datamysqlWriter.set_dataload_hive_sql_filenamePath(hiver.dataload_hive_sql_filenamePath)
        datamysqlWriter.datamysqlAllwriter()

    elif hiver.dataload_type == "hive":
        # hiver.datahiveAllwriter()
        from utils import datahiveWriter
        datahivewriter = datahiveWriter.DATAHIVEWRITER()
        datahivewriter.set_dataload_hive_json_filenamePath(hiver.dataload_hive_json_filenamePath)
        datahivewriter.set_bigdata_name(hiver.name)
        datahivewriter.set_datestring(hiver.datenowstring)
        datahivewriter.analyse_table_fields()
        datahivewriter.set_dataload_time(hiver.dataload_time)
        cmd = "load data local inpath '%s' into table %s.%s partition dt='%s';" % (datahivewriter.get_csv_filepath(),
                                                                                   datahivewriter.dataloader.database,
                                                                                   datahivewriter.dataloader.table_name,
                                                                                   hiver.datenowdate)
        datahivewriter.set_self_cmd(cmd=cmd)
        datahivewriter.read_jsonfile()
    else:
        print("other database need to add!")


if __name__ == '__main__':
    main_one()

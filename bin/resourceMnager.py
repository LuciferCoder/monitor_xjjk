#!/bin/env python3
# -*- coding: utf-8 -*-


import warnings

# 屏蔽本条语句之后所有的告警
import csv

warnings.filterwarnings("ignore")

import os
import json
import sys
import krbticket
from xml.etree import ElementTree as ET
import socket
import paramiko
from datetime import datetime, timedelta

# 数据导入到mysql
import dataLoad

"""
# ldl
# 2023年6月30日15:08:33
"""

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# 数据写入到MySQL


# from EXECUTION.conf import Alarm as Alarm, Logger as LoggerSS
# from conf import Alarm as Alarm, Logger as Logger
from conf import Logger as Logger

"""
        "UsedAMResourceMB", # 当前AM占用的内存
        "UsedAMResourceVCores", # 当前AM占用的CPU
        "AppsRunning", # 当前在运行的APPs
        "AppsPending", # 当前等待调度Apps
        "AppsCompleted", # 当前已经完成Apps
        "AllocatedMB", # 当前占用内存
        "AllocatedVCores", # 当前占用CPU
        "AllocatedContainers", # 当前分配Container数量
        "AvailableMB", # 当前有效剩余内存
        "AvailableVCores", # 当前有效剩余CPU
        "PendingMB", # 当前等待调度需要的内存
        "PendingVCores", # 当前等待调度需要的CPU
        "PendingContainers", # 当前等待调度需要的Container
        "ReservedMB", # 当前预留内存
        "ReservedVCores", # 当前预留CPU
        "ReservedContainers" # 当前预留Container
"""


class ResourceManager():
    # init 初始化内部变量,初始化内部参数
    def __init__(self):
        # yarn_nospace
        # self.dataloadtype = None
        self.availableGaia = None
        # 队列监控
        self.rootQueue_usage_percent = None

        self.rm1_jmx = None
        self.rm2_jmx = None
        self.use_crontab = None
        self.AppsSubmitted = ""
        self.AvailableMB = ""
        self.AvailableVCores = ""
        self.AllocatedVCores = ""
        self.FairShareVCores = ""
        self.FairShareMB = ""
        self.AllocatedMB = ""
        self.BASE_DIR = BASE_DIR
        # self.client = ""
        self.jsonfile_path = self.BASE_DIR + "/conf/yarn/yarn.json"
        name, version, cluster_name, yarnconf, krb5conf, client_keytab, client_keytab_principle, \
        rm1, rm2, rm1_port, rm2_port, nodemanager_list, use_kerberos, ssh_user, ssh_pkey, \
        nodemanagerJmxport, rm1_hostname, rm2_hostname, dataloadtype = self._json_parse()
        self.name = name
        self.version = version
        self.cluster_name = cluster_name
        self.yarnconf = yarnconf
        # self.hdfsconf = yarnconf
        self.krb5conf = krb5conf
        self.client_keytab = client_keytab
        self.client_keytab_principle = client_keytab_principle
        self.rm1 = rm1
        self.rm2 = rm2
        self.rm1_port = rm1_port
        self.rm2_port = rm2_port
        self.nodemanager_list = nodemanager_list
        # 配置文件中namenode节点个数
        self.nodemanager_list_lenth = len(self.nodemanager_list)
        self.use_kerberos = use_kerberos
        self.ssh_user = ssh_user
        self.ssh_pkey = ssh_pkey
        self.nodemanagerJmxport = nodemanagerJmxport
        self.nodemanager_port = self.nodemanagerJmxport
        self.rm1_hostname = rm1_hostname
        self.rm2_hostname = rm2_hostname
        self.dataloadtype = dataloadtype

        self.yarn_site_filepath = self.BASE_DIR + "/conf/yarn/%s" % self.yarnconf
        self.yarnsite_clustername = ""
        self.nodemanager_alive_num_from_jmx = ""

        # 指标抓取的Nodemanagers，返回列表
        self.LiveNodeManagers = []

        # 判断nodemanager alive之后返回的 alive状态的实际列表
        self.alive_ip_host_list = []

        self.nodemanager_gctime_list = []

        # 集群状态不健康
        self.nodemanager_NumUnhealthyNMs = 0

        self.AppsPending = ""
        self.AppsFailed = ""

        # 时间戳信息，脚本开始时进行计算
        # 当前时间： 年月日时分
        self.datenow = datetime.now()
        self.datenowstring = self.datenow.strftime("%Y%m%d%H%M%S")
        # 前一天的当前时间
        # self.lastdayofnow = self.datenow - timedelta(days=1)
        # 前一天的当前时刻
        self.lastdayofnow = self.datenow - timedelta(days=1)
        # self.lasttmofnow = self.datenow - timedelta(minutes=10)
        # 当前时间的十分钟前
        self.lasttmofnow = self.datenow - timedelta(minutes=10)

        # 前一天的当前时间的 年月日时分秒 数字串时间戳
        self.lastdayofnowstring = self.lastdayofnow.strftime("%Y%m%d%H%M%S")

        # 当前时间的十分钟前的 年月日时分秒 数字串时间戳
        self.lastdayofnowstring = self.lasttmofnow.strftime("%Y%m%d%H%M%S")
        # 前一天的年月日
        self.lastdayofdate = self.lasttmofnow.strftime("%Y%m%d")

        ## 指标入库
        self.datenowdate = self.datenow.strftime("%Y%m%d")
        self.datenowtime = self.datenow.strftime("%H%M%S")
        self.lastdayofdate = self.lastdayofnow.strftime("%Y%m%d")
        self.datenowdate = self.datenow.strftime("%Y%m%d")

        # 数据写入本地，并导入mysql
        self.threadnum_list = []
        # 初始化dataload对象
        self.dataloader = dataLoad.DATALOADER()

        self.dataload_yarn_json_dir = self.dataloader.datalaod_yarn_json
        self.dataload_yarn_json_dir_abs = BASE_DIR + self.dataload_yarn_json_dir
        self.dataload_yarn_json_filenamePath = self.dataload_yarn_json_dir_abs + "/%s_yarn_dml.json" % self.datenowstring

        self.dataload_yarn_sql_dir = self.dataloader.datalaod_yarn_sql
        self.dataload_yarn_sql_dir_abs = BASE_DIR + self.dataload_yarn_sql_dir
        self.dataload_yarn_sql_filenamePath = self.dataload_yarn_sql_dir_abs + "/%s_yarn_dml.sql" % self.datenowstring

        self.table_name = self.dataloader.table_name
        # 指标值字典列表，用于dataLoad生成语句

        # nodemanager down 数量
        # 集群节点数(Dead Nodes )	dead_nodes
        self.nodemanager_donwnum = 0
        # 存活节点数	alive_nodes
        self.alive_node_num = 0

        self.AppSubmitted_perday = 0

        # json配置文件分析

        # csv文件写入数据相关
        self.table_field_filepath = BASE_DIR + "/conf/yarn/table_fields.json"
        self.csv_filepath = BASE_DIR + "/csv/%s/%s.csv" % (self.datenowdate, self.datenowdate)

    # 解析配置文件，获取hadoop yarn节点信息(已完成，内部返回类中的变量使用)
    # 创建类的对象时就初始化完成了yarn.json文件的分析
    def _json_parse(self):
        with open(self.jsonfile_path, 'r') as jsonfile:
            load_dict = json.load(jsonfile)

            name = load_dict["name"]
            version = load_dict["version"]

            # 集群名称
            cluster_name = load_dict["dependencies"]["config"]["cluster_name"]

            # hdfs-site.xml
            hdfsconf = load_dict["dependencies"]["config"]["yarnconf"]

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
            rm1_hostname = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm1_hostname"]
            rm2 = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm2"]
            rm2_port = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm2_port"]
            rm2_hostname = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm1_hostname"]

            ##nodemanager
            # dic list
            nodemanager_list = load_dict["dependencies"]["hadoop_nodes"]["nodemanager"]

            # nodemanager_port
            nodemanagerJmxport = load_dict["dependencies"]["hadoop_nodes"]["nodemanagerJmxport"]
            dataloadtype = load_dict["dependencies"]["config"]["dataloadtype"]

            return name, version, cluster_name, hdfsconf, krb5conf, client_keytab, client_keytab_principle, rm1, \
                   rm2, rm1_port, rm2_port, nodemanager_list, use_kerberos, ssh_user, ssh_pkey, nodemanagerJmxport, \
                   rm1_hostname, rm2_hostname, dataloadtype

    # yarn-site.xml文件处理，返回参数
    # 逻辑中尚未用到
    def yarn_site_conf(self):
        try:
            # hdfs_site_file = self.hdfs_site_file
            filepath = self.yarn_site_filepath
            et = ET.parse(filepath)
            node_list = []
            name = ""
            value = ""
            cluster_name = ""
            rm1_ip = ""
            rm2_ip = ""
            nn1_port = ""
            nn2_port = ""

            for node in et.findall("property"):
                # print(node)
                for treenode in node.findall("name"):
                    # print(treenode.text)
                    name = treenode.text
                for treenode in node.findall("value"):
                    # print(treenode.text)
                    value = treenode.text

                st = "%s#%s" % (name, value)
                # print(st)
                node_list.append(st)

            # 获取文件中的集群名称
            for name in node_list:
                if "dfs.nameservices" in name:
                    cluster_name = name.split("#")[1]
                    # print(cluster_name)

            # 获取rm1, rm2
            for name in node_list:
                if "yarn.resourcemanager.webapp.address.rm1" in name:
                    rm1_ip = name.split("#")[1].split(":")[0]
                    rm1_port = name.split("#")[1].split(":")[1]
                    # print(nn1_ip, nn1_port)

                if "yarn.resourcemanager.webapp.address.rm2" in name:
                    rm2_ip = name.split("#")[1].split(":")[0]
                    rm2_port = name.split("#")[1].split(":")[1]
                    # print(nn2_ip, nn2_port)

            # 赋值集群名称
            self.yarnsite_clustername = cluster_name

            return cluster_name, rm1_ip, rm2_ip

        except Exception as e:
            print(e)
        finally:
            pass

    # 认证krb5
    # 如果使用了kerberos，调用此方法
    def krb5init(self):
        krbconf = self.krb5conf
        keytab_file = self.client_keytab
        principle = self.client_keytab_principle
        os.environ['KRB5CCNAME'] = os.path.join(os.path.dirname(self.jsonfile_path), f'keytab/krb5cc_%s' % (self.name))
        kconfig = krbticket.KrbConfig(principal=principle, keytab=keytab_file)
        krbticket.KrbCommand.kinit(kconfig)
        # cont = krbticket.KrbTicket.get(keytab=keytab_file,principal=principle)

    # 端口探活
    # 端口连通性检查
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
            # print(e)
            return "false"

    # 包装远程连接方法，执行命令返回结果
    def ssh_connect(self, ip, port, user, password, use_pwd, ssh_keyfile, cmd):
        try:
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
                # print(result)
                ssh.close()
                bl = ""
                if int(result) == 1:
                    bl = "true"
                else:
                    bl = "false"
                return bl
            else:
                # 使用密钥连接
                ssh.connect(hostname=ip, port=port, username=user, pkey=ssh_key)
                stdin, stdout, stderr = ssh.exec_command(cmd)
                # 输出命令执行结果
                result = stdout.read()
                # print(result)
                ssh.close()
                bl = ""
                if int(result) == 1:
                    bl = "true"
                else:
                    bl = "false"
                return bl
        except Exception as e:
            pass
            # print(e)

    # rn HA 状态分析
    def rm_ha_analyse(self, jmx_cont):
        # jmx_content = jmx_cont
        ha_state = ""
        jmx_content = jmx_cont
        if "This is standby RM" in jmx_content:
            ha_state = "standby"
            return ha_state
        else:
            ha_state = "active"
            return ha_state

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

    # yarn resourceManager 是否宕机
    # 调用 paramikossh
    # 服务进程不存在，且端口探活为失败
    # resourcemanager
    def yarn_resourcemanager_is_down(self, yarn_ip, yarn_port):
        ip = yarn_ip
        port = yarn_port
        pswc_cmd = "ps -ef |grep resourcemanager.ResourceManager|grep -v grep|wc -l"
        user = self.ssh_user
        keyfile_path = self.ssh_pkey

        # 端口探活
        socket_ck_re = self.socket_check(ip=yarn_ip, port=yarn_port)

        # 服务检查
        nn_ssh_result = self.ssh_connect(ip=ip, port=22, password="", use_pwd="false", ssh_keyfile=keyfile_path,
                                         user=user, cmd=pswc_cmd)

        # 此处逻辑：
        # 服务存在且端口正验证正常视为Namenode正常
        # 理论上要先检查Namenode状态之后进行下一步
        if socket_ck_re == "true" and nn_ssh_result == "true":
            print("IP: %s 上的RreourceManager服务状态：Notdown" % yarn_ip)
            return "active"
        else:
            print("IP: %s 上的RreourceManager服务状态：Down" % yarn_ip)
            return "down"

    # namenode jmx接口信息分析处理，获取其他信息的重要函数步骤
    # 主要功能获取两个namenode上额jmx信息
    """
     curl --insecure  https://10.0.0.47:8088/jmx
     可以获取nodemanager node live/dead 
     获取resource manager ha 状态
     返回结果为json格式
     通过 函数 hdfs_site_conf 返回的 nn1 nn2 的ip以及集群名称，进行判断；
     （1）判断集群名称是否与脚本配置文件中的相同，相同则继续检查hdfs文件中的两个ip是否与脚本配置文件中的角色对称
     （2）判断对称之后，获取两个nn的接口信息
    """

    def resourceManager_jmx_info_cx(self, clustername_from_hdfssite, nn1_clustername_from_hdfssite,
                                    nn2_clustername_from_hdfssite):
        # （1）判断集群名称是否与脚本配置文件中的相同，相同则继续检查hdfs文件中的两个ip是否与脚本配置文件中的角色对称
        clustername_from_hdfssite = clustername_from_hdfssite
        clustername_from_config = self.cluster_name

        if clustername_from_config == clustername_from_config:
            # print("cluster name in same, ok.")
            pass
        else:
            print("cluster name is not in same ,faile. 检查配置文件配置与hdfs-site.xml文件集群名称配置")

        if nn1_clustername_from_hdfssite == self.rm1:
            # print(" nn1  is in same, ok.")
            pass
        else:
            print("nn1 ip is not in same, fale. 请检查配置文件中nn1与hdfs-site.xml文件中配置")

        if nn2_clustername_from_hdfssite == self.rm2:
            # print("nn2 is in same, ok.")
            pass
        else:
            print("nn2 ip is not in same, fale. 请检查配置文件中nn2与hdfs-site.xml文件中配置")

        # 检查通过，则进行下一步 ,调用 namenode_api_info
        jsoncont_all_cont1, jsoncont_all_cont2 = self.resourceManager_api_info(nn1_clustername_from_hdfssite,
                                                                               nn2_clustername_from_hdfssite)

        self.rm1_jmx = jsoncont_all_cont1
        self.rm2_jmx = jsoncont_all_cont2

    # 获取nn接口信息,rm1,rm2都需要获取完成的jmx信息
    # def namenode_api_info(self, nn1_clustername_from_hdfssite, nn2_clustername_from_hdfssite):
    def resourceManager_api_info(self, nn1_clustername_from_hdfssite, nn2_clustername_from_hdfssite):
        # print("调用函数 namenode_api_info 成功")
        rm1_ip = nn1_clustername_from_hdfssite
        rm2_ip = nn2_clustername_from_hdfssite

        rm1_port = self.rm1_port
        rm2_port = self.rm2_port

        jsoncont_all_cont1 = ""
        jsoncont_all_cont2 = ""

        # nn1 socket check, return boolean,true-->联通，false-->不连通
        connected_nn1 = self.socket_check(rm1_ip, rm1_port)
        if connected_nn1 == "true":
            cmd = "curl -s   http://%s:%s/jmx" % (rm1_ip, rm1_port)
            jsoncont_all = os.popen(cmd=cmd)
            jsoncont_all_cont1 = jsoncont_all.read()
        else:
            print("端口不通连通性检查不通,请检查服务namenode: %s 是否正常..." % rm1_ip)
            # 此处需要继续执行的，注释掉以下部分
            # exit(201)

        connected_nn2 = self.socket_check(rm2_ip, rm2_port)
        if connected_nn2 == "true":
            cmd = "curl -s  http://%s:%s/jmx" % (rm2_ip, rm2_port)
            jsoncont_all = os.popen(cmd=cmd)
            jsoncont_all_cont2 = jsoncont_all.read()
        else:
            print("端口不通连通性检查不通,请检查服务namenode: %s是否正常..." % rm2_ip)
            # 此处需要继续执行的，注释掉以下部分
            # exit(202)

        return jsoncont_all_cont1, jsoncont_all_cont2

    # resourManager 指标抓取函数 （待修正为RM的抓取指标）
    """
    抓取nodemanager的个数    完成
    
    """

    def nn_jmx_analyse(self, jmx_cont):
        # jmx_content = jmx_cont
        jmx_content = jmx_cont
        jmx_content = json.loads(jmx_content)
        beans = jmx_content["beans"]

        """
        指标入库：
        日期	date	varchar(50)	20230629
        时间	time	varchar(50)	111352
        大数据组件名称	bigdata_component	varchar(50)	大数据组件名称
        存活节点数	alive_nodes
        YARNNodeManager不健康，请检查磁盘空间使用率是否超过90%	NodeManager_healthy
        """
        date = self.datenowdate
        time = self.datenowtime
        bigdata_component = self.name
        alive_nodes = None
        NodeManager_healthy = None

        # print(beans)
        for dic in beans:
            # 开始取指标
            # 取namenode堆内存指标
            """
            抓取nodemanager的个数
            指标：Hadoop:service=ResourceManager,name=ClusterMetrics
            """
            if dic["name"] == "Hadoop:service=ResourceManager,name=ClusterMetrics":
                nodemanager_alive_num = dic["NumActiveNMs"]
                self.nodemanager_alive_num_from_jmx = nodemanager_alive_num
                alive_nodes = nodemanager_alive_num

            """
            抓取 alive 状态的nodemanager列表
            指标：Hadoop:service=ResourceManager,name=RMNMInfo
            """
            if dic["name"] == "Hadoop:service=ResourceManager,name=RMNMInfo":
                LiveNodeManagers = dic["LiveNodeManagers"]
                # print("LiveNodeManagers LiveNodeManagers "+str(type(LiveNodeManagers)))
                # 返回列表
                livenodes = json.loads(LiveNodeManagers)
                # self.LiveNodeManagers = livenodes

            """
            NumUnhealthyNMs
            指标：Hadoop:service=ResourceManager,name=ClusterMetric
            NumUnhealthyNMs
            """
            if dic["name"] == "Hadoop:service=ResourceManager,name=ClusterMetrics":
                nodemanager_NumUnhealthyNMs = dic["NumUnhealthyNMs"]
                self.nodemanager_NumUnhealthyNMs = nodemanager_NumUnhealthyNMs
                # NodeManager_healthy = nodemanager_NumUnhealthyNMs

        # date = self.datenowdate
        # time = self.datenowtime
        # bigdata_component = self.name
        # alive_nodes = None
        # NodeManager_healthy = None

        # json_string = '{"date":"%s","time":"%s","bigdata_component":"%s",' \
        #               '"alive_nodes":"%s","NodeManager_healthy":"%s"}' % (date,
        #                                                                   time,
        #                                                                   bigdata_component,
        #                                                                   alive_nodes,
        #                                                                   NodeManager_healthy)
        # self.jsondata_writer(json_string)

    """
    # yarn nodemanager is down
    # 调用 paramikossh
    # 服务进程不存在，且端口探活为失败
    # nodemanager
    """

    def nodemanager_is_down(self):

        # jmx中的数量与配置文件中的相同：返回nodemanager正常，否则不正常
        if self.nodemanager_alive_num_from_jmx == self.nodemanager_list_lenth:
            print("nodemanager 服务正常")
        else:
            hostname_list = self.nodemanager_list

            hostname_jmx_list = []
            """
            {"HostName":"ocean-bigdata-1a-23",
            "Rack":"/default",
            "State":"RUNNING",
            "NodeId":"ocean-bigdata-1a-23:65033",
            "NodeHTTPAddress":"ocean-bigdata-1a-23:8042",
            "LastHealthUpdate":1687932945367,
            "HealthReport":"",
            "NodeManagerVersion":"3.2.2",
            "NumContainers":0,x`
            "UsedMemoryMB":0,
            "AvailableMemoryMB":10240}
            """
            for hostnamejmx in self.LiveNodeManagers:
                hostname_jmx = hostnamejmx["HostName"]
                hostname_jmx_list.append(hostname_jmx)

            # print("hostname_jmx_list : " + hostname_jmx_list)

            down_ip_host_list = []
            alive_ip_host_list = []

            for hostname_ip in hostname_list:
                hostname = hostname_ip["hostname"]
                ip = hostname_ip["ip"]
                if hostname in hostname_jmx_list:
                    alive_ip_host_list.append(hostname_ip)
                else:
                    # 字符串不在列表中，则为down状态的nodemanager
                    down_ip_host_list.append(hostname_ip)

            # down_ip_host_list 不为0 打印down状态的主机ip
            if len(down_ip_host_list) == 0:
                print("Nodemanager is Down: %s " % str(len(down_ip_host_list)))
            else:
                self.nodemanager_donwnum = len(down_ip_host_list)
                for doonip in down_ip_host_list:
                    ip = doonip["ip"]
                    port = self.nodemanager_port
                    hostname = doonip["ip"]
                    pswc_cmd = "ps -ef |grep nodemanager.NodeManager|grep -v grep|wc -l"
                    user = self.ssh_user
                    keyfile_path = self.ssh_pkey

                    # 端口探活
                    socket_ck_re = self.socket_check(ip=ip, port=port)
                    # print(socket_ck_re)

                    # 服务检查
                    nn_ssh_result = self.ssh_connect(ip=ip, port=22, password="", use_pwd="false",
                                                     ssh_keyfile=keyfile_path,
                                                     user=user, cmd=pswc_cmd)
                    # print(nn_ssh_result)

                    # 此处逻辑：
                    # 服务存在且端口正验证正常视为Namenode正常
                    # 理论上要先检查Namenode状态之后进行下一步
                    if socket_ck_re == "true" and nn_ssh_result == "true":
                        print("IP: %s 上的nodemanager服务状态：Notdown" % ip)
                        # return "alive"
                    else:
                        print("IP: %s 上的nodemanager服务状态：Down" % ip)
                        # return "down"
            self.alive_ip_host_list = alive_ip_host_list
            self.alive_node_num = len(alive_ip_host_list)

            """
            集群信息相关指标 入库
            
            """
            date = self.datenowdate
            time = self.datenowtime
            bigdata_component = self.name
            alive_nodes = self.alive_node_num
            NodeManager_healthy = self.nodemanager_NumUnhealthyNMs
            dead_nodes = self.nodemanager_donwnum

            json_string = '{"date":"%s","time":"%s","bigdata_component":"%s",' \
                          '"alive_nodes":"%s","NodeManager_healthy":"%s","dead_nodes":"%s"}' % (date,
                                                                                                time,
                                                                                                bigdata_component,
                                                                                                alive_nodes,
                                                                                                NodeManager_healthy,
                                                                                                dead_nodes)
            self.jsondata_writer(json_string)

    """
    指标：Nodemanager gc 时间
    Nodemanager 的GC 时间  nodemanager jmx 
    指标key：Hadoop:service=NodeManager,name=JvmMetrics
    关键字：GcTimeMillis
    单位：毫秒
    """

    def get_nodemanager_jmx_info_GcTimeMillis(self, cmd):
        result = os.popen(cmd=cmd)
        result_json = json.loads(result)
        beans = result_json["bean"]
        bean = beans[0]
        bean_json = json.loads(bean)
        result = bean_json["GcTimeMillis"]
        return result

    # nodemanager jmx监控信息处理
    def nodemanager_jmx_analyse(self):
        alive_nodemanagers_list = self.alive_ip_host_list
        nodemanager_port = self.nodemanagerJmxport

        for host_list in alive_nodemanagers_list:
            hostname = host_list["hostname"]
            ip = host_list["ip"]
            # GC Time
            # cmd = "curl http://%s:%s/jmx?qry="Hadoop:service=NodeManager,name=JvmMetrics" %(ip, port)
            cmd = "curl http://%s:%s/jmx?qry=Hadoop:service=NodeManager,name=JvmMetrics" % (ip, nodemanager_port)

            # 调用json处理 get_nodemanager_jmx_info_GcTimeMillis
            gctime = self.get_nodemanager_jmx_info_GcTimeMillis(cmd)
            # print("主机： %s, IP: %s,GCTime 为 %s 毫秒.") % (hostname, ip, gctime)
            self.nodemanager_gctime_list.append('{"hostname": "%s","ip": "%s", "gctime":"%s"}') % (hostname, ip, gctime)

    def nodemanager_gc_time(self):
        nodemanager_gc_time_list = self.nodemanager_gctime_list
        for gctime_host in nodemanager_gc_time_list:
            hostname = gctime_host["gctime_host"]
            ip = gctime_host["gctime_host"]
            gc_time = gctime_host["gctime"]
            print("主机： %s, IP: %s,GCTime 为 %s 毫秒.") % (hostname, ip, gc_time)

            """
            nodemanager gctime指标入库
            日期	date
            时间	time
            ip
            hostname
            大数据组件名称	bigdata_component
            组件服务名称	component_service
            GCTIME/GC时间	gctime
            """
            date = self.datenowdate
            time = self.datenowtime
            bigdata_component = self.name
            component_service = "nodemanager"
            gctime = gc_time
            json_string = '{"hostname":"%s","ip":"%s","date":"%s","time":"%s","bigdata_component":"%s",' \
                          '"component_service":"%s","gctime":"%s"}' % (hostname,
                                                                       ip,
                                                                       date,
                                                                       time,
                                                                       bigdata_component,
                                                                       component_service,
                                                                       gctime)
            self.jsondata_writer(json_string)

    # 队列资源分析
    def queue_analyse_jmx(self, rmip, rmport):
        ip = rmip
        port = rmport
        cmd = "curl -s http://%s:%s/jmx?qry=Hadoop:service=ResourceManager,name=QueueMetrics,q0=root" % (ip, port)
        beans = os.popen(cmd=cmd)
        beans_cont = beans.read()
        beans_json = json.loads(beans_cont)
        bean = beans_json["beans"][0]
        # bean_json = json.loads(bean)

        bean_json = bean
        # 指标：Pengding作业数量
        AppsPending = bean_json["AppsPending"]
        self.AppsPending = AppsPending
        print("Pengding作业数量: %s " % str(self.AppsPending))

        # 指标：失败作业数
        AppsFailed = bean_json["AppsFailed"]
        self.AppsFailed = AppsFailed
        print("失败作业数: %s" % str(self.AppsFailed))

        # 指标： 正在运行的作业数量 AppsRunning
        AppsRunning = bean_json["AppsRunning"]
        self.AppsRunning = AppsRunning
        print("正在运行的作业数量: %s " % self.AppsRunning)

        # FairShareMB 全部内存资源（全部资源）
        FairShareMB = bean_json["FairShareMB"]
        self.FairShareMB = FairShareMB

        # 剩余内存资源 AvailableMB
        AvailableMB = bean_json["AvailableMB"]
        self.AvailableMB = AvailableMB

        # AllocatedMB 使用中的内存资源
        # AllocatedMB = bean_json["AllocatedMB"]
        # 2023年6月28日15:14:37 计算类型转换
        # 已使用的内存资源
        self.AllocatedMB = int(self.FairShareMB) - int(self.AvailableMB)

        # 所有内核数
        FairShareVCores = bean_json["FairShareVCores"]
        self.FairShareVCores = FairShareVCores

        # 剩余的内核数
        AvailableVCores = bean_json["AvailableVCores"]
        self.AvailableVCores = AvailableVCores

        # 使用中的内核数
        # AllocatedVCores = bean_json["AllocatedVCores"]
        self.AllocatedVCores = int(self.FairShareVCores) - int(self.AvailableVCores)

        # 指标：资源使用情况
        print("资源使用情况: \r\n    已使用核数：%s 未使用核数：%s ; 已使用内存: %sMB  剩余内存资源: %sMB" %
              (self.AllocatedVCores, self.AvailableVCores, self.AllocatedMB, self.AvailableMB))

        # 指标：队列资源监控
        # 队列root的使用率
        rootQueue_usage_percent = "{:.2%}".format(float(self.AllocatedMB) / float(self.AvailableMB) / 100)
        print("队列资源监控: root队列使用率 %s " % rootQueue_usage_percent)
        self.rootQueue_usage_percent = rootQueue_usage_percent

        # 指标： YARN上没有足够可分配的资源
        # 低于yarn容器最低资源要求
        if int(self.AvailableVCores) < 1 or int(self.AllocatedMB) < 1024:
            print("YARN上没有足够可分配的资源")
            self.availableGaia = 1
        else:
            self.availableGaia = 0

        # 指标：
        # （6）YRAN计算任务过多
        # （7）YARN计算任务延迟
        # （4）YARN运行的Job过多
        # 其他的467都是pending那个数值大于3就是过多job
        # 和计算任务过多，大于10就算延迟了

        if self.AppsPending == 0:
            pass
        elif self.AppsPending >= 1 and self.AppsPending <= 3:
            print("Yarn 队列Pending 数量：%s" % self.AppsPending)
        elif self.AppsPending > 3 and self.AppsPending <= 10:
            print("YRAN计算任务过多 / YARN运行的Job过多")
        else:
            print("YARN计算任务延迟")

        """
        计算需要的参数值
        （1）提交的App数 
        （2）AppPending 指标：Pengding作业数量
        """
        # 指标参数：AppsSubmitted 提交的App数
        AppsSubmitted = bean_json["AvailableVCores"]
        self.AppsSubmitted = AppsSubmitted

        """
        指标监控入库
        日期	date
        时间	time
        大数据组件名称	bigdata_component
        yarn内核总数	rm_core_total
        yarn内核使用	rm_core_used
        yarn内核可用（剩余）	rm_core_avalable
        yarn内存总数	rm_mem_total
        yarn内存使用	rm_mem_use
        yarn内存可用（剩余）	rm_mem_available
        正在运行的作业数量	Apprunning
        失败作业数	AppFailed
        提交的作业数总数	AppSubmitted
        Pengding作业数量	AppPending
        yarn_nospace
        # 新增 队列资源监控
        rootqueue_usage_percent

        """
        date = self.datenowdate
        time = self.datenowtime
        bigdata_component = self.name
        rm_core_total = self.FairShareVCores
        rm_core_used = self.AllocatedVCores
        rm_core_avalable = self.AvailableVCores
        rm_mem_total = self.FairShareMB
        rm_mem_use = self.AllocatedMB
        rm_mem_available = self.AvailableMB
        Apprunning = self.AppsRunning
        AppFailed = self.AppsFailed
        AppSubmitted = self.AppsSubmitted
        AppPending = self.AppsPending
        yarn_nospace = self.availableGaia
        rootqueue_usage_percent = self.rootQueue_usage_percent

        json_string = '{"date":"%s","time":"%s","bigdata_component":"%s","rm_core_total":"%s","rm_core_used":"%s",' \
                      '"rm_core_avalable":"%s","rm_mem_total":"%s","rm_mem_use":"%s","rm_mem_available":"%s",' \
                      '"Apprunning":"%s","AppFailed":"%s","AppSubmitted":"%s",' \
                      '"AppPending":"%s","yarn_nospace":"%s","rootqueue_usage_percent":"%s"}' % (date, time,
                                                                                                 bigdata_component,
                                                                                                 rm_core_total,
                                                                                                 rm_core_used,
                                                                                                 rm_core_avalable,
                                                                                                 rm_mem_total,
                                                                                                 rm_mem_use,
                                                                                                 rm_mem_available,
                                                                                                 Apprunning,
                                                                                                 AppFailed,
                                                                                                 AppSubmitted,
                                                                                                 AppPending,
                                                                                                 yarn_nospace,
                                                                                                 rootqueue_usage_percent)
        self.jsondata_writer(json_string)

    # 待编辑计算的参数
    # （1）YARN任务排队超过10min
    # （11）每天用户提交的作业数

    """
    指标： YARN任务排队超过10min
    写json数据文件
    """

    def job_pendding_tenminitues(self):
        # 获取当前时间戳数字串
        curtime = self.datenowstring
        # 获取十分钟前的时间戳字符串
        tenmago = self.lasttmofnow
        # Pending状态的作业
        pendingAppNum = self.AppsPending
        # 当前时间截止提交的Apps
        AppSubmitted = self.AppsSubmitted

        # 判断运行方式
        use_crontab = self.use_crontab

        # 数据格式 json
        # {"pendingAppNum":"0","AppSubmitted":"0"}
        json_str = '{"AppsPending":"%s","AppsSubmitted":"%s"}' % (pendingAppNum, AppSubmitted)

        # 定时任务数据写入定开
        # if use_crontab == "true":
        #     # 写入文件全部指标数据的字符串
        #     json_str = '{"AppsPending":"%s","AppsSubmitted":"%s"，"":"%s}' % (pendingAppNum, AppSubmitted)

        # 文件名称 20230622184504_crontab_hdfs_capcity.json
        filename = "%s_crontab_yarn_Apps.json" % curtime
        path = BASE_DIR + "/cron/yarn/%s" % filename

        # 数据入库字段，有排队队列超过十分钟使数字等于排队队列数值
        """
        指标值入库
        """
        Apppending_longten = 0

        # 判断pending状态的作业
        # 当前为0 直接写入文件
        if int(pendingAppNum) == 0:
            with open(path, 'w', encoding="utf-8") as file:
                file.write(json_str)
                file.close()
            # 写入文件数据到当前时间戳文件
            # 可以选择不打印此处
            Apppending_longten = pendingAppNum
            print("YARN任务排队数量: %s ,写入json数据文件到本地" % pendingAppNum)
        else:
            # pendding数不为0,读取十分钟前的数据
            # 前提是文件存在，不存在则为第一次计算，打印当前，不计算，写入当前数据到文件
            new_file = "%s_crontab_yarn_Apps.json" % tenmago
            new_filepath = BASE_DIR + "/cron/yarn/%s" % new_file

            if os.path.exists(new_filepath):
                # 写当天数据到本地，保存数据
                with open(path, 'w', encoding="utf-8") as file:
                    file.write(json_str)
                    file.close()

                # 10分钟前的文件存在，读取十分钟前的文件数据，进行判断，10分钟前的文件
                with open(new_filepath, 'r', encoding="utf-8") as reader:
                    file_cont = reader.read()
                    reader_json = json.loads(file_cont)
                    AppsPending = reader_json["AppsPending"]
                    if int(AppsPending) != 0:
                        print("YARN任务排队超过10min, 当前排队数量: %s " % str(pendingAppNum))
                        Apppending_longten = pendingAppNum

            else:
                # 10分钟前的文件不存在，直接写数据到本地
                with open(new_filepath, 'w', encoding="utf-8") as file:
                    file.write(json_str)
                    file.close()
                print("没有获取到10分钟前的数据文件,YARN任务排队数量: %s ,请注意检查处理" % pendingAppNum)

        """
        指标入库
        YARN任务排队超过10min	Apppending_longten
        日期	date
        时间	time
        大数据组件名称	bigdata_component
        """
        date = self.datenowdate
        time = self.datenowtime
        bigdata_component = self.name
        Apppending_longten = Apppending_longten
        json_string = '{"date":"%s","time":"%s","bigdata_component":"%s","Apppending_longten":"%s"}' % (date, time,
                                                                                                        bigdata_component,
                                                                                                        Apppending_longten)
        self.jsondata_writer(json_string)

    # 取文件，年月日时分所在文件
    """
    指标：每天用户提交的作业数
    """

    def job_submitted_calculate(self):
        # 获取当前时间戳数字串
        curtime = self.datenowstring
        # 获取十分钟前的时间戳字符串
        ondaygo = self.lasttmofnow
        # Pending状态的作业
        pendingAppNum = self.AppsPending
        # 当前时间截止提交的Apps
        AppSubmitted = self.AppsSubmitted

        # 数据格式 json
        # {"pendingAppNum":"0","AppSubmitted":"0"}
        json_str = '{"AppsPending":"%s","AppsSubmitted":"%s"}' % (pendingAppNum, AppSubmitted)

        # 文件名称 20230622184504_crontab_hdfs_capcity.json
        filename = "%s_crontab_yarn_Apps.json" % curtime
        path = BASE_DIR + "/cron/yarn/%s" % filename

        lastday_filename = "%s_crontab_yarn_Apps.json" % self.lastdayofnowstring
        lastday_filename_path = BASE_DIR + "/cron/yarn/%s" % lastday_filename

        # 判断24小时前的文件存不存在，存在读取文件计算；不存在则打印当前已提交的作业总数
        if os.path.exists(lastday_filename_path):
            try:
                with open(lastday_filename, 'r', encoding="utf-8") as reader:
                    reader_cont = reader.read()
                    reader_cont_json = json.loads(reader_cont)
                    AppSubmitted_lastday = reader_cont_json["AppsSubmitted"]
                    self.AppSubmitted_perday = AppSubmitted_lastday

                    # 计算作业数
                    abs_num = int(AppSubmitted_lastday) - int(AppSubmitted)
                    print("每天用户提交的作业数: %s" % str(abs_num))
            except Exception as e:
                print(e)
        else:
            self.AppSubmitted_perday = AppSubmitted
            print("前一天相同时间 %s 没有记录的数据存在！当前时间 %s 已提交作业数：%s" % (
                str(self.datenow), str(self.lastdayofnow), AppSubmitted))

        """
        指标入库
        每天用户提交的作业数	AppSubmitted_perday
        日期	date
        时间	time
        大数据组件名称	bigdata_component
        """
        date = self.datenowdate
        time = self.datenowtime
        bigdata_component = self.name
        AppSubmitted_perday = self.AppSubmitted_perday

        json_string = '{"date":"%s","time":"%s","bigdata_component":"%s",' \
                      '"AppSubmitted_perday":"%s"}' % (date, time,
                                                       bigdata_component,
                                                       AppSubmitted_perday)
        self.jsondata_writer(json_string)

    # 此方法可以后期重构
    def dataAllwriter(self):
        print("dataAllwriter: ","dataAllwriter")
        sqllist = []
        # 字典
        jsonfile = self.dataload_yarn_json_filenamePath
        # "insert into table(key1, key2, key3) values(value1,value2,value3);"
        sqlfile = open(self.dataload_yarn_sql_filenamePath, 'a', encoding="utf-8")
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
        if self.dataloadtype == "mysql":
            # print("dataloadtype: ", self.dataloadtype)
            self.dataloader.loaddata_main()
        elif self.dataloadtype == "hive":
            # print("dataloadtype: ",self.dataloadtype)
            self.jsondataAllfields_writer()
        else:
            print(" self.dataloadtype: ", self.dataloadtype)
            exit(600)

    # 数据写入本地json文件
    # 在其他住区指标的地方，拼接指标dic串，调用此方法写入到本地文件中
    def jsondata_writer(self, dicstring):
        jsonfile = self.dataload_yarn_json_filenamePath
        with open(jsonfile, 'a', encoding="utf-8") as file:
            file.write(dicstring + "\n")
            file.close()

    def jsondataAllfields_writer(self):
        jsonfile = self.dataload_yarn_json_filenamePath
        # 读取hive的所有字段
        tablefiledpath = self.table_field_filepath
        file = open(tablefiledpath, 'r', encoding='utf-8')
        tablefiledsjson = json.load(file)
        tablefile_list = tablefiledsjson["fields"]
        # fields_list_dic = "{" + '"' + '":"NULL","'.join(tablefile_list) + '":"NULL"' + "}\n"
        # fields_list_dic = json.loads(fields_list_dic)
        # print("tablefiledsdic: ",fields_list_dic)

        with open(jsonfile, 'r', encoding="utf-8") as file:
            lines = file.readlines()
            for line in lines:
                jsoncont = json.loads(str(line).replace("\'", "\""))
                jsonfile_keys = jsoncont.keys()
                fields_dic = "{" + '"' + '":"NULL","'.join(tablefile_list) + '":"NULL"' + "}\n"
                fields_dic = json.loads(fields_dic)
                # print("fields_dic: ", fields_dic)

                for key in jsonfile_keys:
                    value = str(jsoncont[key]).replace("{}", "NULL")
                    # fields_list_dic[key] = jsonfile_cont[key]
                    fields_dic["%s" % key] = value

                # print("fields_dic_added: ", fields_dic)
                csv_filepath = self.csv_filepath

                # 如果日期文件夹不存在，创建日期目录
                if not os.path.exists(os.path.dirname(csv_filepath)):
                    os.mkdir(os.path.dirname(csv_filepath))

                # 将文件写入到csv文件中
                with open(csv_filepath, 'a', encoding='utf-8') as jsonfile:
                    csv_writer = csv.writer(jsonfile)
                    csv_writer.writerow(fields_dic.values())
                    jsonfile.close()


# 主函数逻辑
def main_one():
    # 对象化
    resourmanager = ResourceManager()

    # 参数分析
    resourmanager.arg_analyse()

    # 初始化kerberos
    if resourmanager.use_kerberos == "true":
        resourmanager.krb5init()

    # resourcemanager is down （完成）
    # 测试namenode
    rm1_state = resourmanager.yarn_resourcemanager_is_down(resourmanager.rm1, resourmanager.rm1_port)
    rm2_state = resourmanager.yarn_resourcemanager_is_down(resourmanager.rm2, resourmanager.rm2_port)

    # 服务状态不正常的情况下打印提醒退出程序，此处可以编辑发送邮件提醒/短信提醒
    if rm1_state == "down":
        print("rm1 resourceManager服务疑似 down，请检查！")
        """
        指标：服务状态检查数据写入
        日期	date
        时间	time
        大数据组件名称	bigdata_component
        组件服务名称	component_service
        IP	ip
        主机名	hostname
        服务存活	component_service_status 0/1,0存活，1down，可以使用count来显示down的节点数
        """
        ip = resourmanager.rm1
        hostname = resourmanager.rm1_hostname
        date = resourmanager.datenowdate
        time = resourmanager.datenowtime
        bigdata_component = resourmanager.name
        component_service = "resourmanager"
        component_service_status = 1

        json_string = '{"ip":"%s","hostname":"%s","date":"%s","time":"%s","bigdata_component":"%s",' \
                      '"component_service":"%s",' \
                      '"component_service_status":"%s"}' % (ip,
                                                            hostname,
                                                            date,
                                                            time,
                                                            bigdata_component,
                                                            component_service,
                                                            component_service_status)
        resourmanager.jsondata_writer(json_string)

        exit(301)
    else:
        # 写入指标
        print("rm1 resourceManager服务 started ！")
        """
        指标：服务状态检查数据写入
        日期	date
        时间	time
        大数据组件名称	bigdata_component
        组件服务名称	component_service
        IP	ip
        主机名	hostname
        服务存活	component_service_status 0/1,0存活，1down，可以使用count来显示down的节点数
        """
        ip = resourmanager.rm1
        hostname = resourmanager.rm1_hostname
        date = resourmanager.datenowdate
        time = resourmanager.datenowtime
        bigdata_component = resourmanager.name
        component_service = "resourmanager"
        component_service_status = 0

        json_string = '{"ip":"%s","hostname":"%s","date":"%s","time":"%s","bigdata_component":"%s",' \
                      '"component_service":"%s",' \
                      '"component_service_status":"%s"}' % (ip,
                                                            hostname,
                                                            date,
                                                            time,
                                                            bigdata_component,
                                                            component_service,
                                                            component_service_status)
        resourmanager.jsondata_writer(json_string)

    if rm2_state == "down":
        print("rm1 resourceManager服务疑似 down， 请检查！")
        """
        指标：服务状态检查数据写入
        日期	date
        时间	time
        大数据组件名称	bigdata_component
        组件服务名称	component_service
        IP	ip
        主机名	hostname
        服务存活	component_service_status 0/1,0存活，1down，可以使用count来显示down的节点数
        """
        ip = resourmanager.rm2
        hostname = resourmanager.rm2_hostname
        date = resourmanager.datenowdate
        time = resourmanager.datenowtime
        bigdata_component = resourmanager.name
        component_service = "resourmanager"
        component_service_status = 1

        json_string = '{"ip":"%s","hostname":"%s","date":"%s","time":"%s","bigdata_component":"%s",' \
                      '"component_service":"%s",' \
                      '"component_service_status":"%s"}' % (ip,
                                                            hostname,
                                                            date,
                                                            time,
                                                            bigdata_component,
                                                            component_service,
                                                            component_service_status)
        resourmanager.jsondata_writer(json_string)
        exit(302)
    else:
        # 写入指标
        """
        指标：服务状态检查数据写入
        日期	date
        时间	time
        大数据组件名称	bigdata_component
        组件服务名称	component_service
        IP	ip
        主机名	hostname
        服务存活	component_service_status 0/1,0存活，1down，可以使用count来显示down的节点数
        """
        ip = resourmanager.rm2
        hostname = resourmanager.rm2_hostname
        date = resourmanager.datenowdate
        time = resourmanager.datenowtime
        bigdata_component = resourmanager.name
        component_service = "resourmanager"
        component_service_status = 1

        json_string = '{"ip":"%s","hostname":"%s","date":"%s","time":"%s","bigdata_component":"%s",' \
                      '"component_service":"%s",' \
                      '"component_service_status":"%s"}' % (ip,
                                                            hostname,
                                                            date,
                                                            time,
                                                            bigdata_component,
                                                            component_service,
                                                            component_service_status)
        resourmanager.jsondata_writer(json_string)

    # 处理yarn-site.xml文件返回值
    # cluster_name, rm1_ip, rm2_ip = resourmanager.hdfs_site_conf()
    cluster_name, rm1_ip, rm2_ip = resourmanager.yarn_site_conf()

    # 检查配置文件信息核对，核对之后调用 namenode_api_info 返回jmx信息值
    resourmanager.resourceManager_jmx_info_cx(cluster_name, rm1_ip, rm2_ip)

    # 分析jmx指标，打印指标状态
    # nn_jmx_analyse
    # nn1 jmx
    rm1_ha_state = resourmanager.rm_ha_analyse(resourmanager.rm1_jmx)
    rm2_ha_state = resourmanager.rm_ha_analyse(resourmanager.rm2_jmx)

    if rm1_ha_state == "active" and rm2_ha_state == "standby":
        jmx_cont = resourmanager.rm1_jmx
        resourmanager.nn_jmx_analyse(jmx_cont)
    elif rm1_ha_state == "standby" and rm2_ha_state == "active":
        jmx_cont = resourmanager.rm2_jmx
        resourmanager.nn_jmx_analyse(jmx_cont)
    else:
        print(
            "resourceManager HA 状态检查异常：rm1_ha_state 为 %s ;  rm2_ha_state 为 %s .请运维立即检查所有resourceManager状态，并进行恢复！" % (
                rm1_ha_state, rm2_ha_state))
        exit(502)

    # 指标：nodemanager is down（完成）
    resourmanager.nodemanager_is_down()

    # nodemanger gc time
    """
    Nodemanager 的GC时间
    单位：ms
    完成
    """
    resourmanager.nodemanager_gc_time()

    # 指标 nodemanager不健康 (完成)
    if int(resourmanager.nodemanager_NumUnhealthyNMs) == 0:
        pass
    else:
        print("YARNNodeManager不健康，请检查磁盘空间使用率是否超过90%")

    # rmjmx 队列相关的
    # resourmanager.queue_analyse_jmx()
    if rm1_ha_state == "active" and rm2_ha_state == "standby":
        ip = rm1_ip
        port = resourmanager.rm1_port
        resourmanager.queue_analyse_jmx(ip, port)
    elif rm1_ha_state == "standby" and rm2_ha_state == "active":
        ip = rm2_ip
        port = resourmanager.rm2_port
        resourmanager.queue_analyse_jmx(ip, port)
    else:
        print(
            "resourceManager HA 状态检查异常：rm1_ha_state 为 %s ;  rm2_ha_state 为 %s .请运维立即检查所有resourceManager状态，并进行恢复！")
        exit(502)

    # 指标：YARN任务排队超过10min
    resourmanager.job_pendding_tenminitues()

    # 指标： 每天用户提交的作业数
    resourmanager.job_submitted_calculate()

    # 数据导入到mysql
    resourmanager.dataAllwriter()


# 主函数
if __name__ == '__main__':
    main_one()

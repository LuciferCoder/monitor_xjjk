#!/bin/env python3
# -*- coding: utf-8 -*-


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

"""
resourceManager 检测指标
（1）YARN任务排队超过10min
（2）YARN上没有足够可分配的资源
（3）YARNNodeManager不健康，请检查磁盘空间使用率是否超过90%
（4）YARN运行的Job过多
（5）队列资源监控
（6）YRAN计算任务过多
（7）YARN计算任务延迟
（8）YarnNodeManager is down
（9）YarnResourceManager is down
（10）Pengding作业数量
（11）每天用户提交的作业数
（12）失败作业数
（13）正在运行的作业数量
（14）资源使用情况
（15）Nodemanager 的GC 时间
"""

"""
# 李冬亮
# 03月24日 00:08
"""

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# from EXECUTION.conf import Alarm as Alarm, Logger as Logger
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
        self.BASE_DIR = BASE_DIR
        # self.client = ""
        self.jsonfile_path = self.BASE_DIR + "/conf/yarn/yarn.json"
        name, version, cluster_name, hdfsconf, krb5conf, client_keytab, client_keytab_principle, rm1, rm2, rm1_port, rm2_port, nodemanager_list, use_kerberos, ssh_user, ssh_pkey = self._json_parse()
        self.name = name
        self.version = version
        self.cluster_name = cluster_name
        self.hdfsconf = hdfsconf
        self.krb5conf = krb5conf
        self.client_keytab = client_keytab
        self.client_keytab_principle = client_keytab_principle
        self.rm1 = rm1
        self.rm2 = rm2
        self.rm1_port = rm1_port
        self.rm2_port = rm2_port
        self.nodemanager_list = nodemanager_list
        self.use_kerberos = use_kerberos
        self.ssh_user = ssh_user
        self.ssh_pkey = ssh_pkey

        self.yarn_site_filepath = self.BASE_DIR + "/conf/yarn/yarn-site.xml"
        self.yarnsite_clustername = ""

    # json配置文件分析
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

            return name, version, cluster_name, hdfsconf, krb5conf, client_keytab, client_keytab_principle, rm1, rm2, rm1_port, rm2_port, nodemanager_list, use_kerberos, ssh_user, ssh_pkey

    # yarn-site.xml文件处理，返回参数
    # 逻辑中尚未用到
    def hdfs_site_conf(self):
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

                if "yarn.resourcemanager.webapp.address.rm2"  in name:
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
            print(e)
            return "false"


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
            ssh.close()
            bl = ""
            if int(result) == 1:
                bl = "true"
            else:
                bl = "false"
            return bl

    # rn HA 状态分析
    def rm_ha_analyse(self, jmx_cont):
        # jmx_content = jmx_cont
        ha_state = ""
        jmx_content = jmx_cont
        if "This is standby RM" in jmx_content:
            ha_state = "standby"
            return ha_state
        else:
            ha_state = "alive"
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
        port =  yarn_port
        pswc_cmd = "ps -ef |grep resourcemanager.ResourceManager|grep -v grep|wc -l"
        user = self.ssh_user
        keyfile_path = self.ssh_pkey

        # 端口探活
        socket_ck_re = self.socket_check(ip=yarn_ip,port=yarn_port)

        # 服务检查
        nn_ssh_result = self.ssh_connect(ip=ip, port=22, password="", use_pwd="false", ssh_keyfile=keyfile_path,
                                          user=user, cmd=pswc_cmd)

        # 此处逻辑：
        # 服务存在且端口正验证正常视为Namenode正常
        # 理论上要先检查Namenode状态之后进行下一步
        if socket_ck_re == "true" and nn_ssh_result == "true":
            print("IP: %s 上的NameNode服务状态：Notdown" % yarn_ip)
            return "alive"
        else:
            print("IP: %s 上的NameNode服务状态：Down" % yarn_ip)
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

        self.nn1_jmx = jsoncont_all_cont1
        self.nn2_jmx = jsoncont_all_cont2


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
            cmd = "curl -s   https://%s:%s/jmx" % (rm1_ip, rm1_port)
            jsoncont_all = os.popen(cmd=cmd)
            jsoncont_all_cont1 = jsoncont_all.read()
        else:
            print("端口不通连通性检查不通,请检查服务namenode: %s 是否正常..." % rm1_ip)
            # 此处需要继续执行的，注释掉以下部分
            # exit(201)

        connected_nn2 = self.socket_check(rm2_ip, rm2_port)
        if connected_nn2 == "true":
            cmd = "curl -s  https://%s:%s/jmx" % (rm2_ip, rm2_port)
            jsoncont_all = os.popen(cmd=cmd)
            jsoncont_all_cont2 = jsoncont_all.read()
        else:
            print("端口不通连通性检查不通,请检查服务namenode: %s是否正常..." % rm2_ip)
            # 此处需要继续执行的，注释掉以下部分
            # exit(202)

        return jsoncont_all_cont1, jsoncont_all_cont2



# resourManager 指标抓取函数 （待修正为RM的抓取指标）
    def nn_jmx_analyse(self, jmx_cont):
        # jmx_content = jmx_cont
        jmx_content = jmx_cont
        jmx_content = json.loads(jmx_content)
        beans = jmx_content["beans"]
        # print(beans)
        for dic in beans:
            # 开始取指标
            # 取namenode堆内存指标
            """
            指标： Hadoop:service=NameNode,name=JvmMetrics
            百分比：MemHeapUsedM/MemHeapCommittedM
            Namenode 堆内存使用率
            HeapMemoryUsage.used/HeapMemoryUsage.committed
            """
            if dic["name"] == "Hadoop:service=NameNode,name=JvmMetrics":
                MemHeapUsedM = dic["MemHeapUsedM"]
                MemHeapCommittedM = dic["MemHeapCommittedM"]
                # 百分比计算
                namenode_heap_used_percent = "{:.2%}".format(MemHeapUsedM / MemHeapCommittedM)


                # 测试判断逻辑
                # namenode_heap_used_percent = "77.49%"
                # namenode_heap_used_percent = "91.49%"

                # 判断返回指标：
                if float(namenode_heap_used_percent.strip("%")) >= 70 and float(
                        namenode_heap_used_percent.strip("%")) < 90:
                    print("HDFSNameNode堆内存使用率超过70%")
                elif float(namenode_heap_used_percent.strip("%")) >= 90:
                    print("HDFSNameNode堆内存使用率超过90%")
                else:
                    print("HDFSNameNode堆内存使用率为%s" % namenode_heap_used_percent)

            """
            空间使用率: 采集指标
            PercentUsed
            """
            if dic["name"] == "Hadoop:service=NameNode,name=NameNodeInfo":
                PercentUsed = "{:.2%}".format(dic["PercentUsed"] / 100)

                # 测试判断逻辑
                # PercentUsed = "77.49%"
                # PercentUsed = "91.49%"

                # 判断返回指标：
                if float(PercentUsed.strip("%")) >= 70 and float(PercentUsed.strip("%")) < 90:
                    print("HDFS使用率超过70%")
                elif float(PercentUsed.strip("%")) >= 90:
                    print("HDFS使用率超过90%")
                else:
                    print("HDFS使用率 %s" % PercentUsed)

                """
                liveNodes node: liveNodes
                DeadNodes
                打印数量
                """
                # #Hadoop:service=NameNode,name=NameNodeInfo
                # liveNodes 指标
                # DeadNodes
                LiveNodes = dic["LiveNodes"]
                # 字符串转字典格式
                # livenodes = eval(LiveNodes)
                livenodes = json.loads(LiveNodes)
                hosts = livenodes.keys()
                # 存活节点数：livenodes_num
                livenodes_num = len(hosts)
                print("LiveNodes 数量： %s" % (livenodes_num))

                """
                DeadNodes node: DeadNodes
                DeadNodes
                打印IP?
                """
                DeadNodes = dic["DeadNodes"]
                # 字符串转字典格式
                deadnodes = json.loads(DeadNodes)
                hosts = deadnodes.keys()
                # 存活节点数：deadnodes
                deadnnode_num = len(hosts)
                print("DeadNodes 数量： %s" % (deadnnode_num))


                """
                datanode is down
                """
                deadnnode_list = []
                if deadnnode_num != 0:
                    for host_ip in deadnodes.keys():
                        host, port = host_ip.split(":")
                        ip = deadnodes.get(host_ip).get("xferaddr").split(":")[0]
                        host_ip = "%s#%s" % (host, ip)
                        deadnnode_list.append(host_ip)

                for host_ip in deadnnode_list:
                    hostname, ip = host_ip.split("#")
                    # use_crontab == false,打印到console，定时任务只抓取DeadNodes指标,
                    # 可以使用手动执行的巡检方式打印出DeadNodes主机以及IP
                    if self.use_crontab == "false":
                        print("deadnode 主机名：%s ip: %s is down." % (hostname, ip))

            """
            集群DN节点磁盘(Total Datanode Volume Failures）
            采集指标：VolumeFailuresTotal
            Hadoop:service=NameNode,name=FSNamesystem
            """
            if dic["name"] == "Hadoop:service=NameNode,name=FSNamesystem":
                # 集群DN节点磁盘(Total Datanode Volume Failures）
                VolumeFailuresTotal = dic["VolumeFailuresTotal"]
                print("Total Datanode Volume Failures: %s " % VolumeFailuresTotal)

            """
            Hadoop:service=NameNode,name=NameNodeStatus
            HA状态
            """
            if dic["name"] == "Hadoop:service=NameNode,name=NameNodeStatus":
                # 判断当前节点ha状态
                HostAndPort = dic["HostAndPort"]
                host, port = HostAndPort.split(":")
                State = dic["State"]
                print("主机： %s namenode的HA状态为： %s " % (host, State))

            """
            CapacityUsed
            采集指标：CapacityUsed
            Hadoop:service=NameNode,name=FSNamesystem
            """
            if dic["name"] == "Hadoop:service=NameNode,name=FSNamesystem":
                # 判断当前节点ha状态
                CapacityUsed = dic["CapacityUsed"]
                CapacityUsedGB = int(CapacityUsed) / 1024 / 1024 / 1024
                self.curday_cap = CapacityUsedGB



def main_one():
    # 对象化
    resourmanager = ResourceManager()

    # 参数分析
    resourmanager.arg_analyse()

    # 初始化kerberos
    if resourmanager.use_kerberos == "true":
        resourmanager.krb5init()

    # resourcemanager is down
    # 测试namenode
    rm1_state = resourmanager.Namenode_is_down(resourmanager.rm1, resourmanager.rm1_port)
    rm2_state = resourmanager.Namenode_is_down(resourmanager.rm2, resourmanager.rm2_port)

    # 服务状态不正常的情况下打印提醒退出程序，此处可以编辑发送邮件提醒/短信提醒
    if rm1_state == "down":
        print("nn1 namenode服务疑似 down，请检查！")
        exit(301)

    if rm2_state == "down":
        print("nn2 namenode服务疑似 down， 请检查！")
        exit(302)

    # 处理yarn-site.xml文件返回值
    cluster_name, rm1_ip, rm2_ip = resourmanager.hdfs_site_conf()

    # 检查配置文件信息核对，核对之后调用 namenode_api_info 返回jmx信息值
    resourmanager.resourceManager_jmx_info_cx(cluster_name, rm1_ip, rm2_ip)

    # 分析jmx指标，打印指标状态
    # nn_jmx_analyse
    # nn1 jmx
    rm1_ha_state = resourmanager.rm_ha_analyse(resourmanager.nn1_jmx)
    rm2_ha_state = resourmanager.rm_ha_analyse(resourmanager.nn2_jmx)

    if rm1_ha_state == "active" and rm2_ha_state =="standby":
        jmx_cont = resourmanager.nn1_jmx
        resourmanager.nn_jmx_analyse(jmx_cont)
    elif rm1_ha_state == "standby" and rm2_ha_state =="active":
        jmx_cont = resourmanager.nn2_jmx
        resourmanager.nn_jmx_analyse(jmx_cont)
    else:
        print("resourceManager HA 状态检查异常：rm1_ha_state 为 %s ;  rm2_ha_state 为 %s .请运维立即检查所有resourceManager状态，并进行恢复！")
        exit(502)





# 主函数
if __name__ == '__main__':
    main_one()

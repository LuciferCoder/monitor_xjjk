#!/bin/env python3
# -*- coding: utf-8 -*-

"""
脚本检查项
五、hdfs
（1）HDFS使用率超过70%
（2）HDFS使用率超过85%
（3）HDFSNameNode堆内存使用率超过70%
（4）HDFSNameNode堆内存使用率超过90%
（5）DataNode is down             #获取node信息
（6）NameNode is down             #获取node信息
（7）HDFS日增数据量
（8）Namenode HA的状态               #jmx
（9）HDFS健康检查
（10）集群节点数(Live Nodes)       #获取node信息
（11）集群节点数(Dead Nodes )      #获取node信息
（12）集群DN节点磁盘(Total Datanode Volume Failures）
"""

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

"""带有Kerberos认证的hdfs认证"""
"""
1、获取认证Kerberos认证信息进行认证
2、执行hdfs的命令
"""

"""
from pyhive import hive
from krbcontext.context import krbContext


with krbContext(using_keytab=True, principal="ocean/ocean@JD.COM", keytab_file="/root/ocean.keytab"):
  cnn=hive.Connection(host='10.0.0.45', port=10000, database='ods', auth="KERBEROS", kerberos_service_name='hive')
  cursor=cnn.cursor()
  stmt="select * from ods_my49_tbnd_a_d"
  cursor.execute(stmt)
  data=cursor.fetchall()
  cnn.close()
  for result in data:
    print(result)

# 李冬亮
# 03月24日 00:08
# @袁慧 你明天白天测试的时候仿照这个案例去测试，principal="ocean/ocean@JD.COM", keytab_file="/root/ocean.keytab" 改成对应环境的值，
# cnn=hive.Connection(host='10.0.0.45', port=10000, database='ods' 这里面的值改成对应环境值，测试一下；有问题明天及时发到群里
"""

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# from EXECUTION.conf import Alarm as Alarm, Logger as Logger
# from conf import Alarm as Alarm, Logger as Logger
from conf import Logger as Logger

warnings.filterwarnings('ignore')


class HDFSCHECk():

    def __init__(self):
        self.BASE_DIR = BASE_DIR
        self.client = ""
        self.jsonfile_path = self.BASE_DIR + "/conf/hdfs/hdfs.json"
        name, version, cluster_name, hdfsconf, krb5conf, client_keytab, client_keytab_principle, nn1, nn2, nn1_port, nn2_port, datanode_list, use_kerberos, ssh_user, ssh_pkey = self._json_parse()
        self.name = name
        self.version = version
        self.cluster_name = cluster_name
        self.krb5conf = krb5conf
        self.client_keytab = client_keytab
        self.client_keytab_principle = client_keytab_principle
        self.nn1 = nn1
        self.nn2 = nn2
        self.nn1_port = nn1_port
        self.nn2_port = nn2_port
        self.datanode_list = datanode_list
        self.hdfsconf = hdfsconf
        self.hdfsconf_path = os.path.dirname(self.jsonfile_path) + "/" + self.hdfsconf
        # hdfs_nodes数量，来自于健康检查返回的节点数
        self.hdfs_node_count = 0
        self.use_kerberos = use_kerberos
        self.hdfs_site_file = "/conf/hdfs/hdfs-site.xml"
        self.hdfs_site_filepath = self.BASE_DIR + self.hdfs_site_file
        self.hdfssite_clustername = ""
        # namenode jmx信息
        self.nn1_jmx = ""
        self.nn2_jmx = ""
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

    # 解析配置文件，获取hadoop节点信息(已完成，内部返回类中的变量使用)
    def _json_parse(self):
        with open(self.jsonfile_path, 'r') as jsonfile:
            load_dict = json.load(jsonfile)

            name = load_dict["name"]
            version = load_dict["version"]
            dependencies = load_dict["dependencies"]

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
            ## nn
            nn1 = load_dict["dependencies"]["hadoop_nodes"]["namenode"]["nn1"]
            nn1_port = load_dict["dependencies"]["hadoop_nodes"]["namenode"]["nn1_port"]
            nn2 = load_dict["dependencies"]["hadoop_nodes"]["namenode"]["nn2"]
            nn2_port = load_dict["dependencies"]["hadoop_nodes"]["namenode"]["nn2_port"]

            ssh_user = load_dict["dependencies"]["config"]["ssh_user"]
            ssh_pkey = load_dict["dependencies"]["config"]["ssh_pkey"]

            # ##rm
            # rm1 = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm1"]
            # rm1_port = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm1_port"]
            # rm2 = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm2"]
            # rm2_port = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm2_port"]

            ##datanode
            datanode_list = load_dict["dependencies"]["hadoop_nodes"]["datanode"]

            ##nodemanager
            # nodemanager_list = load_dict["dependencies"]["hadoop_nodes"]["nodemanager"]

            return name, version, cluster_name, hdfsconf, krb5conf, client_keytab, client_keytab_principle, nn1, nn2, nn1_port, nn2_port, datanode_list, use_kerberos, ssh_user, ssh_pkey

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


    # hdfs 健康检查
    """
   HDFS健康的标准:如果所有的文件满足最小副本的要求，那么就认为文件系统是健康的。
   # 重要
    """

    def hdfs_health_check(self):
        try:
            hdfsconfpath = self.hdfsconf_path
            cmd = "hdfs fsck -conf %s /" % hdfsconfpath
            foo = os.popen(cmd=cmd)

            filename = "/conf/hdfs/%sfsck.log" % self.datenowstring
            file_path = BASE_DIR + filename

            with open(file_path, 'w',encoding="utf-8") as file:
                file.write(foo.read())
                file.close()


            with open(file_path, 'r', encoding="utf-8") as foo:
                for lin in foo.readlines():
                    if "The filesystem under path " in lin:

                        lin = lin.replace("The filesystem under path ", "").split(" is ")
                        hdfs_path = lin[0]
                        hdfs_path_state = lin[1]
                        # print(hdfs_path,hdfs_path_state)
                        # hdfs_path_state = "CORRUPT"
                        if hdfs_path_state == "HEALTHY":
                            print("HDFS路径 %s 健康状态检查结果为 %s" % (hdfs_path, hdfs_path_state))
                        else:
                            print("HDFS路径 %s 健康状态检查结果为 %s , 请检查hdfs块信息健康状态并进行处理!!!" % (
                                hdfs_path, hdfs_path_state))

                    # 抓取hdfsnode数量：
                    if "Number of data-nodes:" in lin:
                        node_num = lin.replace("Number of data-nodes:", "").strip()
                        self.hdfs_node_count = node_num
                        # print(self.hdfs_node_count)
        except Exception as e:
            print(e)

    # hdfs-site.xml文件处理，返回参数
    def hdfs_site_conf(self):
        try:
            # hdfs_site_file = self.hdfs_site_file
            filepath = self.hdfs_site_filepath
            et = ET.parse(filepath)
            node_list = []
            name = ""
            value = ""
            cluster_name = ""
            nn1_ip = ""
            nn2_ip = ""
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

            # 获取nn1, nn2
            for name in node_list:
                if "dfs.namenode.rpc-address.%s.nn1" % cluster_name in name:
                    nn1_ip = name.split("#")[1].split(":")[0]
                    nn1_port = name.split("#")[1].split(":")[1]
                    # print(nn1_ip, nn1_port)

                if "dfs.namenode.rpc-address.%s.nn2" % cluster_name in name:
                    nn2_ip = name.split("#")[1].split(":")[0]
                    nn2_port = name.split("#")[1].split(":")[1]
                    # print(nn2_ip, nn2_port)

            self.hdfssite_clustername = cluster_name

            return cluster_name, nn1_ip, nn2_ip

        except Exception as e:
            print(e)
        finally:
            pass

    # namenode jmx接口信息分析处理，获取其他信息的重要函数步骤
    # 主要功能获取两个namenode上额jmx信息
    """
     curl --insecure  https://10.0.0.45:50470/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem
     可以获取datanode live/dead
     获取namenode ha 状态
     返回结果为json格式
     通过 函数 hdfs_site_conf 返回的 nn1 nn2 的ip以及集群名称，进行判断；
     （1）判断集群名称是否与脚本配置文件中的相同，相同则继续检查hdfs文件中的两个ip是否与脚本配置文件中的角色对称
     （2）判断对称之后，获取两个nn的接口信息
    """

    def namenode_jmx_info_cx(self, clustername_from_hdfssite, nn1_clustername_from_hdfssite,
                             nn2_clustername_from_hdfssite):
        # （1）判断集群名称是否与脚本配置文件中的相同，相同则继续检查hdfs文件中的两个ip是否与脚本配置文件中的角色对称
        clustername_from_hdfssite = clustername_from_hdfssite
        clustername_from_config = self.cluster_name

        if clustername_from_config == clustername_from_config:
            # print("cluster name in same, ok.")
            pass
        else:
            print("cluster name is not in same ,faile. 检查配置文件配置与hdfs-site.xml文件集群名称配置")

        if nn1_clustername_from_hdfssite == self.nn1:
            # print(" nn1  is in same, ok.")
            pass
        else:
            print("nn1 ip is not in same, fale. 请检查配置文件中nn1与hdfs-site.xml文件中配置")


        if nn2_clustername_from_hdfssite == self.nn2:
            # print("nn2 is in same, ok.")
            pass
        else:
            print("nn2 ip is not in same, fale. 请检查配置文件中nn2与hdfs-site.xml文件中配置")


        # 检查通过，则进行下一步 ,调用 namenode_api_info
        jsoncont_all_cont1, jsoncont_all_cont2 = self.namenode_api_info(nn1_clustername_from_hdfssite,
                                                                        nn2_clustername_from_hdfssite)

        self.nn1_jmx = jsoncont_all_cont1
        self.nn2_jmx = jsoncont_all_cont2

    # 获取nn接口信息,nn1,nn2都需要获取完成的jmx信息
    def namenode_api_info(self, nn1_clustername_from_hdfssite, nn2_clustername_from_hdfssite):
        # print("调用函数 namenode_api_info 成功")
        nn1_ip = nn1_clustername_from_hdfssite
        nn2_ip = nn2_clustername_from_hdfssite

        nn1_port = self.nn1_port
        nn2_port = self.nn2_port

        jsoncont_all_cont1 = ""
        jsoncont_all_cont2 = ""

        # nn1 socket check, return boolean,true-->联通，false-->不连通
        connected_nn1 = self.socket_check(nn1_ip, nn1_port)
        if connected_nn1 == "true":
            cmd = "curl -s --insecure  https://%s:%s/jmx" % (nn1_ip, nn1_port)
            jsoncont_all = os.popen(cmd=cmd)
            jsoncont_all_cont1 = jsoncont_all.read()
        else:
            print("端口不通连通性检查不通,请检查服务namenode: %s 是否正常..." % nn1_ip)
            # 此处需要继续执行的，注释掉以下部分
            # exit(201)

        connected_nn2 = self.socket_check(nn2_ip, nn2_port)
        if connected_nn1 == "true":
            cmd = "curl -s --insecure  https://%s:%s/jmx" % (nn2_ip, nn2_port)
            jsoncont_all = os.popen(cmd=cmd)
            jsoncont_all_cont2 = jsoncont_all.read()
        else:
            print("端口不通连通性检查不通,请检查服务namenode: %s是否正常..." % nn2_ip)
            # 此处需要继续执行的，注释掉以下部分
            # exit(202)

        return jsoncont_all_cont1, jsoncont_all_cont2

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

    # jmx信息处理，取得参数，计算
    """
    （1）HDFS使用率超过70%
    （2）HDFS使用率超过85%
    （3）HDFSNameNode堆内存使用率超过70%
    （4）HDFSNameNode堆内存使用率超过90%
    （5）DataNode is down
    （6）NameNode is down
    （7）HDFS日增数据量
    （8）Namenode HA的状态
    （9）HDFS健康检查
    （10）集群节点数(Live Nodes)
    （11）集群节点数(Dead Nodes )
    （12）集群DN节点磁盘(Total Datanode Volume Failures）

    """


    """
    namenode_ha 状态：
    
    """
    def nn_ha_analyse(self, jmx_cont):
        # jmx_content = jmx_cont
        jmx_content = jmx_cont
        jmx_content = json.loads(jmx_content)
        beans = jmx_content["beans"]
        # print(beans)
        for dic in beans:
            """
            抓取namnode state状态
            tag.HAState
            """
            if dic["name"] == "Hadoop:service=NameNode,name=FSNamesystem":
                ha_state = dic["tag.HAState"]
                return ha_state


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

    """
    指标抓取单项检查：
    namenode服务是否 down
    NameNode is down
    综合检查，首要检查服务是否宕机，namenode正常才进行其他检查
    """

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

    """
    NameNoed is donw
    指标： 服务进程 && 端口探活
    """

    def Namenode_is_down(self, namenode_ip, namenode_port):
        #
        # def hdfs_usage_calculate(self):
        ip = namenode_ip
        port = namenode_port

        pswc_cmd = "ps -ef |grep namenode.NameNode|grep -v grep|wc -l"
        user = self.ssh_user
        keyfile_path = self.ssh_pkey

        nn_ssh_result = self.ssh_connect(ip=ip, port=22, password="", use_pwd="false", ssh_keyfile=keyfile_path,
                                          user=user, cmd=pswc_cmd)
        socket_ck_re = self.socket_check(ip=ip, port=port)
        # 此处逻辑：
        # 服务存在且端口正验证正常视为Namenode正常
        # 理论上要先检查Namenode状态之后进行下一步
        if nn_ssh_result == "true" and socket_ck_re == "true":
            print("IP: %s 上的NameNode服务状态：Notdown" % namenode_ip)
            return "alive"
        else:
            print("IP: %s 上的NameNode服务状态：Down" % namenode_ip)
            return "down"


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

    # 判断是手动执行还是定时任务执行,增加巡检脚本执行方式
    # true 为定时任务执行
    # false 为手动执行
    # 手动执行取定时任务获取的hdfs增长量(打印截止时间： 示例：日期时分秒_crontab_hdfs_capcity.json)
    # 文件检查前一天的时分是否存在,定时任务运行时,获取前一天的文件记录的变量值
    """
    计算 HDFS日增长
    """
    def diff_of_hdfsAdded(self):
        # 前一天文件路径
        # 定时任务执行
        use_crontab = self.use_crontab
        file_lastname = "_crontab_hdfs_capcity.json"
        lastdayofnowstring = self.lastdayofnowstring

        # 打印时间戳信息
        # print("当前日期时间戳: " + self.datenowstring)
        # print("前一天日期时间戳: " + self.lastdayofnowstring)

        # 本地测试
        # lastdayofnowstring = "20230622185554"

        hdfs_cron_dir = "/cron/hdfs/%s%s" % (lastdayofnowstring, file_lastname)
        abs_path_of_file = BASE_DIR + hdfs_cron_dir

        # 前一天文件路径检查
        file_exsited = os.path.exists(abs_path_of_file)
        # print(file_exsited)

        # 定时任务执行时
        # 本地测试
        # use_crontab = "true"
        if use_crontab == "true":
            # 判断前一天文件在不在
            # 如果文件存在，读取前一天文件值，进行计算
            # 文件格式：
            # {growuped: "0",hdfscap: "0"}
            # file_exsited = "True"
            if file_exsited.__str__() == "True":
                # 前一天数据
                lastday_cap = ""
                hdfs_cap_added = ""
                # 当日获取的数据
                # 在获取指标的部分获取到具体的值
                curday_cap = self.curday_cap
                curday_cap_str = "{:.2%}".format(curday_cap / 100).replace("%", "GB")
                # print(curday_cap_str)

                with open(abs_path_of_file, 'r', encoding="utf-8") as file:
                    lastday_cap_json = json.load(file)
                    # 1408.71GB
                    lastday_cap = lastday_cap_json["hdfscap"]

                    print("lastday_cap_jsonf-->" + lastday_cap)
                    file.close()

                # 计算(fload类型)
                # print("curday_cap_str--> " + curday_cap_str)
                # print("lastday_cap--> " + lastday_cap)
                hdfs_cap_added = float(curday_cap_str.replace("GB", "")) - float(lastday_cap.replace("GB", ""))
                # 将当天算得的数据写入记录文件
                # file_lastname = "_crontab_hdfs_capcity.json"
                curdayofnowstring = self.datenowstring
                hdfs_cron_dir_cur = "/cron/hdfs/%s%s" % (curdayofnowstring, file_lastname)
                abs_path_of_file_cur = BASE_DIR + hdfs_cron_dir_cur
                with open(abs_path_of_file_cur, 'w') as f:
                    dic = '{"growuped": "%s", "hdfscap": "%s"}' % (str(hdfs_cap_added) + "GB", curday_cap_str)
                    # json.dump(dic, f)
                    f.write(dic)
                    f.close()

            else:
                # 如果不存在，写入当前获取的数据到当前日期的文件中，跳过计算，设置/返回增长量为当前获取的hdfs ca指标数值
                # print(file_exsited.__str__())
                # file_lastname = "_crontab_hdfs_capcity.json"
                # 文件不存在，直接写入当天数据到本地，设置grouadd的值与hdfs cap 值相等
                curdayofnowstring = self.datenowstring
                hdfs_cron_dir_cur = "/cron/hdfs/%s%s" % (curdayofnowstring, file_lastname)
                abs_path_of_file_cur = BASE_DIR + hdfs_cron_dir_cur

                curday_cap = self.curday_cap
                curday_cap_str = "{:.2%}".format(curday_cap / 100).replace("%", "GB")
                # print("前一天文件不存在-->" + curday_cap_str)
                hdfs_cap_added = curday_cap_str

                with open(abs_path_of_file_cur, 'w', encoding="utf-8") as f:
                    dic = '{"growuped": "%s", "hdfscap": "%s"}' % (hdfs_cap_added, curday_cap_str)
                    # json.dump(dic, f)
                    f.write(dic)
                    f.close()

        # 判定为手动执行， false时
        else:
            # 判定为手动执行， false时
            # 手动执行时 hdfs added 只打印增长量

            # 理出存放地的所有文件名，获取前一天日期（“年月日”）所在的文件
            hdfs_cron_dir = "/cron/hdfs"
            abs_path = BASE_DIR + hdfs_cron_dir
            # fileslist = os.path.dirname(abs_path)
            dir_list = os.listdir(abs_path)

            # 设置一个布尔值变量作为确认文件是否存在的标志，默认为不存在 False
            file_exsited = False
            lastfilepath = ""

            # 可能会有多个文件的时候，取最后一个运行的文件
            listedfile = []
            for filename in dir_list:
                if self.lastdayofdate in filename:
                    listedfile.append(filename)

            listedfile.sort()

            if len(listedfile) != 0:
                lastfile = listedfile.pop()
                # os.path.join方法需要注意
                lastfilepath = os.path.join(abs_path, lastfile)
                file_exsited = True
            else:
                file_exsited = False

            # 本地测试
            # file_exsited = False

            if file_exsited.__str__() == "True":
                # 前一天数据
                lastday_cap = ""
                hdfs_cap_added = ""
                # 当日获取的数据
                # 在获取指标的部分获取到具体的值
                curday_cap = self.curday_cap
                curday_cap_str = "{:.2%}".format(curday_cap / 100).replace("%", "GB")

                with open(lastfilepath, 'r', encoding="utf-8") as file:
                    lastday_cap_json = json.load(file)
                    lastday_cap = lastday_cap_json["hdfscap"]
                    file.close()

                # 计算(fload类型)
                hdfs_cap_added = float(curday_cap_str.replace("GB", "")) - float(lastday_cap.replace("GB", ""))
                # 手动执行脚本模式，打印增长量到console
                print("HFDS增长量为: %sGB" % hdfs_cap_added)
            else:
                # 如果不存在，则视为第一次运行
                # print("file_exsited.__str__() --> " + file_exsited.__str__())
                # file_lastname = "_crontab_hdfs_capcity.json"
                # 文件不存在，直接写入当天数据到本地，设置grouadd的值与hdfs cap 值相等
                curdayofnowstring = self.datenowstring
                hdfs_cron_dir_cur = "/cron/hdfs/%s%s" % (curdayofnowstring, file_lastname)
                abs_path_of_file_cur = BASE_DIR + hdfs_cron_dir_cur

                curday_cap = self.curday_cap
                curday_cap_str = "{:.2%}".format(curday_cap / 100).replace("%", "GB")
                print("前一天文件不存在,当前HDFS使用量为： %s " % curday_cap_str)
                # hdfs_cap_added = curday_cap_str


# 主函数逻辑
def main_one():
    # 初始化,初始化之后，函数内部会读取配置文件进行解析，完成初始化内部变量
    checker = HDFSCHECk()
    # 初始化分析参数，完成use_crontab值的内部变量赋值
    checker.arg_analyse()
    # 测试namenode
    nn1_state = checker.Namenode_is_down(checker.nn1, checker.nn1_port)
    nn2_state = checker.Namenode_is_down(checker.nn2, checker.nn2_port)

    # 服务状态不正常的情况下打印提醒退出程序，此处可以编辑发送邮件提醒/短信提醒
    if nn1_state == "down":
        print("nn1 namenode服务疑似 down，请检查！")
        exit(301)

    if nn2_state == "down":
        print("nn2 namenode服务疑似 down， 请检查！")
        exit(302)

    # 处理hdfs-site.xml文件返回值
    cluster_name, nn1_ip, nn2_ip = checker.hdfs_site_conf()
    # 检查配置文件信息核对，核对之后调用 namenode_api_info 返回jmx信息值
    checker.namenode_jmx_info_cx(cluster_name, nn1_ip, nn2_ip)

    # 分析jmx指标，打印指标状态
    # nn_jmx_analyse
    # nn1 jmx
    nn1_ha_state = checker.nn_ha_analyse(checker.nn1_jmx)
    nn2_ha_state = checker.nn_ha_analyse(checker.nn2_jmx)

    if nn1_ha_state == "active" and nn2_ha_state =="standby":
        jmx_cont = checker.nn1_jmx
        checker.nn_jmx_analyse(jmx_cont)
    elif nn1_ha_state == "standby" and nn2_ha_state =="active":
        jmx_cont = checker.nn2_jmx
        checker.nn_jmx_analyse(jmx_cont)
    else:
        print("namenode HA 状态检查异常：nn1_ha_state 为 %s ;  nn2_ha_state 为 %s .请运维立即检查所有Namenode状态，并进行恢复")
        exit(502)

    checker.diff_of_hdfsAdded()

    # 进行健康检查
    checker.krb5init()
    checker.hdfs_health_check()


if __name__ == '__main__':
    main_one()

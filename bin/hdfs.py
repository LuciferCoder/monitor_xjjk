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

import os
import json
import sys
# import krbcontext
import hdfs
import krbticket
from xml.etree import ElementTree as ET
import socket

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


class HDFSCHECk():

    def __init__(self):
        self.BASE_DIR = BASE_DIR
        self.client = ""
        self.jsonfile_path = self.BASE_DIR + "/conf/hdfs/hdfs.json"
        name, version, cluster_name, hdfsconf, krb5conf, client_keytab, client_keytab_principle, nn1, nn2, nn1_port, nn2_port, datanode_list, use_kerberos = self._json_parse()
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
            # print(user_kerberos)

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

            # ##rm
            # rm1 = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm1"]
            # rm1_port = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm1_port"]
            # rm2 = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm2"]
            # rm2_port = load_dict["dependencies"]["hadoop_nodes"]["resourcemanager"]["rm2_port"]

            ##datanode
            datanode_list = load_dict["dependencies"]["hadoop_nodes"]["datanode"]

            ##nodemanager
            # nodemanager_list = load_dict["dependencies"]["hadoop_nodes"]["nodemanager"]

            # print(datanode_list)
            # print(nodemanager_list)
            # print(type(datanode_list))

            return name, version, cluster_name, hdfsconf, krb5conf, client_keytab, client_keytab_principle, nn1, nn2, nn1_port, nn2_port, datanode_list, use_kerberos

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
        # print(cont)

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
            # for lin in foo.readlines():
            #     if "The filesystem under path " in lin:
            #         print(lin)
            with open(self.BASE_DIR + "/conf/hdfs/fsck.log") as foo:
                for lin in foo.readlines():
                    if "The filesystem under path " in lin:
                        print(lin)
                        lin = lin.replace("The filesystem under path ", "").split(" is ")
                        # print("replcaed: " + lin)
                        # print(lin)
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
                        print(self.hdfs_node_count)
        except Exception as e:
            print(e)

    # hdfs-site.xml文件处理
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
                print(st)
                node_list.append(st)

            # 获取文件中的集群名称
            for name in node_list:
                if "dfs.nameservices" in name:
                    cluster_name = name.split("#")[1]
                    print(cluster_name)

            # 获取nn1, nn2
            for name in node_list:
                if "dfs.namenode.rpc-address.%s.nn1" % cluster_name in name:
                    nn1_ip = name.split("#")[1].split(":")[0]
                    nn1_port = name.split("#")[1].split(":")[1]
                    print(nn1_ip, nn1_port)

                if "dfs.namenode.rpc-address.%s.nn2" % cluster_name in name:
                    nn2_ip = name.split("#")[1].split(":")[0]
                    nn2_port = name.split("#")[1].split(":")[1]
                    print(nn2_ip, nn2_port)

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
            print("cluster name in same, ok.")
        else:
            print("cluster name is not in same ,faile. 检查配置文件配置与hdfs-site.xml文件集群名称配置")
            exit(101)

        if nn1_clustername_from_hdfssite == self.nn1:
            print(" nn1  is in same, ok.")
        else:
            print("nn1 ip is not in same, fale. 请检查配置文件中nn1与hdfs-site.xml文件中配置")
            exit(102)

        if nn2_clustername_from_hdfssite == self.nn2:
            print("nn2 is in same, ok.")
        else:
            print("nn2 ip is not in same, fale. 请检查配置文件中nn2与hdfs-site.xml文件中配置")
            exit(103)

        # 检查通过，则进行下一步 ,调用 namenode_api_info
        jsoncont_all_cont1, jsoncont_all_cont2 = self.namenode_api_info(nn1_clustername_from_hdfssite,
                                                                        nn2_clustername_from_hdfssite)

        self.nn1_jmx = jsoncont_all_cont1
        self.nn2_jmx = jsoncont_all_cont2

    # 获取nn接口信息,nn1,nn2都需要获取完成的jmx信息
    def namenode_api_info(self, nn1_clustername_from_hdfssite, nn2_clustername_from_hdfssite):
        print("调用函数 namenode_api_info 成功")
        nn1_ip = nn1_clustername_from_hdfssite
        nn2_ip = nn2_clustername_from_hdfssite

        nn1_port = self.nn1_port
        nn2_port = self.nn2_port

        jsoncont_all_cont1 = ""
        jsoncont_all_cont2 = ""

        # nn1 socket check, return boolean,true-->联通，false-->不连通
        connected_nn1 = self.socket_check(nn1_ip, nn1_port)
        # print(connected_nn1)
        if connected_nn1 == "true":
            cmd = "curl -s --insecure  https://%s:%s/jmx" % (nn1_ip, nn1_port)
            jsoncont_all = os.popen(cmd=cmd)
            jsoncont_all_cont1 = jsoncont_all.read()
        else:
            print("端口不通连通性检查不通,请检查服务namenode: %s 是否正常..." % nn1_ip)
            # 此处需要继续执行的，注释掉以下部分
            exit(201)

        connected_nn2 = self.socket_check(nn2_ip, nn2_port)
        if connected_nn1 == "true":
            cmd = "curl -s --insecure  https://%s:%s/jmx" % (nn2_ip, nn2_port)
            jsoncont_all = os.popen(cmd=cmd)
            jsoncont_all_cont2 = jsoncont_all.read()
        else:
            print("端口不通连通性检查不通,请检查服务namenode: %s是否正常..." % nn2_ip)
            # 此处需要继续执行的，注释掉以下部分
            exit(202)

        return jsoncont_all_cont1, jsoncont_all_cont2

    # 端口连通性检查
    def socket_check(self, ip, port):
        print("调用函数 socket_check 成功")
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

    def nn_jmx_analyse(self, jmx_cont):
        # jmx_content = jmx_cont
        jmx_content = jmx_cont
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
                # print(namenode_heap_used_percent)

                #测试判断逻辑
                # namenode_heap_used_percent = "77.49%"
                # namenode_heap_used_percent = "91.49%"

                # 判断返回指标：
                if float(namenode_heap_used_percent.strip("%")) >= 70 and float(namenode_heap_used_percent.strip("%")) < 90:
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
                PercentUsed = "{:.2%}".format(dic["PercentUsed"]/100)
                # print(PercentUsed)

                #测试判断逻辑
                # PercentUsed = "77.49%"
                # PercentUsed = "91.49%"

                # 判断返回指标：
                if float(PercentUsed.strip("%")) >= 70 and float(PercentUsed.strip("%")) < 90:
                    print("HDFS使用率超过70%")
                elif float(PercentUsed.strip("%")) >= 90:
                    print("HDFS使用率超过90%")
                else:
                    print("HDFS使用率超过%s" % PercentUsed)


                """
                liveNodes node: liveNodes
                DeadNodes
                打印数量
                """
                # #Hadoop:service=NameNode,name=NameNodeInfo
                # liveNodes 指标
                # DeadNodes
                LiveNodes = dic["LiveNodes"]
                #字符串转字典格式
                # livenodes = eval(LiveNodes)
                livenodes = json.loads(LiveNodes)
                hosts=livenodes.keys()
                # 存活节点数：livenodes_num
                livenodes_num = len(hosts)
                print("LiveNodes 数量： %s" % (livenodes_num))



                """
                DeadNodes node: DeadNodes
                DeadNodes
                打印IP?
                """
                DeadNodes = dic["DeadNodes"]
                #字符串转字典格式
                deadnodes = json.loads(DeadNodes)
                hosts=deadnodes.keys()
                # 存活节点数：deadnodes
                deadnnode_num = len(hosts)
                print("DeadNodes 数量： %s" % (deadnnode_num))

                # # hosts = dict.keys(livenodes)
                # # print(hosts)
                # for host_port in hosts:
                #     host = host_port.split(":")[0]
                #     port = host_port.split(":")[1]
                #     value = livenodes.get(host_port)
                #     print(value)
                #     # value_ofhost = eval(value)
                #     ipaddr = value["infoAddr"].split(":")[0]
                #     print(ipaddr)
                #     exit(0)

                """
                datanode is down
                """
                deadnnode_list = []
                if deadnnode_num != 0:
                    for host_ip in deadnodes.keys():
                        host, port = host_ip.split(":")
                        # print(host)
                        # print(port)
                        ip = deadnodes.get(host_ip).get("xferaddr").split(":")[0]
                        # print(ip)
                        host_ip = "%s#%s" %(host, ip)
                        deadnnode_list.append(host_ip)

                for host_ip in deadnnode_list:
                    hostname, ip = host_ip.split("#")
                    print("deadnode 主机名：%s ip: %s is down." % (hostname, ip))








            """
            集群DN节点磁盘(Total Datanode Volume Failures）
            采集指标：VolumeFailuresTotal
            Hadoop:service=NameNode,name=FSNamesystem
            """
            if dic["name"] == "Hadoop:service=NameNode,name=FSNamesystem":
                # 集群DN节点磁盘(Total Datanode Volume Failures）
                VolumeFailuresTotal = dic["VolumeFailuresTotal"]
                print("集群DN节点磁盘(Total Datanode Volume Failures）数量: %s " % VolumeFailuresTotal)

                #



    # 获取datanode信息
    """
    进行一步节点数量与配置检查的节点数量检查,通过jmx获取datanode的节点数量来进行判断
    """

    def datanode_info(self):
        pass

    # 计算使用率
    def hdfs_usage_calculate(self):
        pass

    # namenode ha check
    def namenode_ha_check(self):
        pass

    # namenode heap mem check
    def namenode_heap_mem_check(self):
        pass


def main():
    # 初始化
    checker = HDFSCHECk()
    # name, version, cluster_name, krb5conf, client_keytab, client_keytab_principle, nn1, nn2, rm1, rm2, datanode_list, nodemanager_list = checker._json_parse()

    # checker.hdfs_health_check()
    # cluster_name, nn1_ip, nn2_ip = checker.hdfs_site_conf()
    # checker.namenode_jmx_info_cx(cluster_name, nn1_ip, nn2_ip)

    # 本地测试
    nn1_jmx = ""
    with open(BASE_DIR + "/conf/hdfs/45jmx_with_deaddatanodes.json", 'r') as file:
        nn1_jmx = json.load(file)
        # print(nn1_jmx)

    checker.nn_jmx_analyse(nn1_jmx)


if __name__ == '__main__':
    main()

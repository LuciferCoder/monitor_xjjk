#!/bin/env python3
# -*- coding: utf-8 -*-
# date: 2023年7月18日10:38:11
# 通用类
# 功能： Hive数据库写入
#


"""重组hive导入文件格式，写入文件到csv文件，之后执行导入"""

import os
import sys

import json
import csv

from bin import dataLoad

import shutil

# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

"""
csv文件格式
逗号分隔

# 建表语句
create table monitorxjjk(
date string comment '6位日期数值，例:20230712 指2023年7月12日',
time string comment '6位时间数值，例:104400 值10点44分00秒',
bigdata_component string comment '大数据组件名称，例如 hadoop',
component_service string comment '大数据组件服务名称，例如 hdfs,yarn,hiveserver2等',
hdfs_usage string comment 'HDFS使用率',
ip string comment '服务器IP地址',
hostname string comment '服务器主机名称',
heap_usage string comment '堆内存使用率',
component_service_status int comment '大数据组件服务状态,0/1,0存活，1down，可以使用count来显示down的节点数',
hdfs_added string comment 'HDFS日增数据量',
namenode_ha_status int comment 'Namenode HA的状态,1/2,1为active,2为standby',
hdfs_healthy int comment 'hdfs_健康检查（1/-1）(健康、不健康)',
alive_nodes int  comment '存活节点数',
dead_nodes int  comment 'Dead Nodes/宕机节点数',
volume_failure int comment '集群DN节点磁盘数量',
gctime int comment 'GCTIME/GC时间',
client_num int comment 'Hvieserver2的连接客户端数量',
rm_core_total int comment 'yarn内核总数',
rm_core_used int comment 'yarn内核已使用数',
rm_core_avalable int comment 'yarn内核可用剩余数量',
rm_mem_total int comment 'yarn内存总数,单位：MB',
rm_mem_use int comment 'yarn内存使用,单位：MB',
rm_mem_available int comment 'yarn内存可用剩余大小,单位：MB',
Apprunning int comment '正在运行的作业数量',
AppFailed int comment '失败作业数',
AppSubmitted int comment '提交的作业数总数',
AppSubmitted_perday int comment '每天用户提交的作业数',
AppPending int comment 'Pengding作业数量',
NodeManager_healthy int comment 'YARNNodeManager不健康,请检查磁盘空间使用率是否超过90%,0/1,0健康,1不健康',
yarn_nospace int comment 'YARN上没有足够可分配的资源,0/1,0资源足够，1没有足够资源',
Apppending_longten int comment 'YARN任务排队超过10min',
rootqueue_usage_percent string  comment '队列资源监控，字符串，root队列使用率百分比'
) partitioned by (dt string comment '按天分区，值等同于字段 date_st')
row format delimited fields terminated by ',';
"""


class DATAHIVEWRITER(object):

    def __init__(self):
        self.dataload_time = None
        self.cmd = None
        self.json_path = "/csv"
        self.bigdata_name = None
        self.table_fields_json = "/conf/hivePD/table_fields.json"
        self.table_fields_jsonpath = BASE_DIR + self.table_fields_json
        # json数据文件
        self.dataload_hive_json_filenamePath = None
        self.datestring = None
        # /csv/202307192313.csv
        # self.csv_filepath = BASE_DIR + "%s/%s/%s.csv" % (self.json_path,
        #                                                  self.get_datestring()[0:8],
        #                                                  self.get_datestring())
        self.csv_filepath = None
        # self.final_csv_filepath = BASE_DIR + "/%s/%s.scv" % (self.json_path, self.datestring)
        self.final_csv_filepath = self.csv_filepath
        self.table_fields_list = None

        # "/dataload/csv/%s/%s_%s.json"
        self.csv_file = None

        self.dataloader = dataLoad.DATALOADHIVER()
        self.hiveserver2_ip = None
        self.hiveserver2_port = None

        # 认证所需要的的参数
        self.name = None
        self.client_keytab_principle = None
        self.client_keytab = None
        self.krb5conf = None

        # hdfs到时导入的文件为前一天的数据文件
        self.laste_date = None

    def set_laste_date(self,laste_date):
        self.laste_date = laste_date

    def get_laste_date(self):
        return self.laste_date

    def set_csv_filepath(self):
        self.csv_filepath = BASE_DIR + "%s/%s/%s.csv" % (self.json_path,
                                                         str(self.get_datestring())[0:8],
                                                         str(self.get_datestring())[0:8])

    def get_csv_filepath(self):
        return self.csv_filepath

    def set_cluster_name(self, name):
        self.name = name

    def get_cluster_name(self):
        return self.name

    def set_client_keytab_principle(self, client_keytab_principle):
        self.client_keytab_principle = client_keytab_principle

    def get_client_keytab_principle(self):
        return self.client_keytab_principle

    def set_krb5conf(self, krb5conf):
        self.krb5conf = krb5conf

    def get_krb5conf(self):
        return self.krb5conf

    def set_client_keytab(self, krb5conf):
        self.krb5conf = krb5conf

    def get_client_keytab(self):
        return self.krb5conf

    def set_final_csv_filepath(self, final_csv_filepath):
        self.final_csv_filepath = final_csv_filepath

    def get_final_csv_filepath(self):
        return self.final_csv_filepath

    def set_csv_file(self, csv_file):
        self.csv_file = csv_file

    def get_csv_file(self):
        return self.csv_file

    def set_self_cmd(self, cmd):
        self.cmd = cmd

    def get_csv_filepath(self):
        return self.csv_filepath

    def set_hiveserver2_ip(self, hiveserver2_ip):
        self.hiveserver2_ip = hiveserver2_ip

    def get_hiveserver2_ip(self):
        return self.hiveserver2_ip

    def set_hiveserver2_port(self, hiveserver2_port):
        self.hiveserver2_port = hiveserver2_port

    def get_hiveserver2_port(self):
        return self.hiveserver2_port

    # 传入值 dataload_hive_json_filenamePath
    def set_dataload_hive_json_filenamePath(self, dataload_hive_json_filenamePath):
        self.dataload_hive_json_filenamePath = dataload_hive_json_filenamePath

    # 返回值 dataload_hive_json_filenamePath
    def get_dataload_hive_json_filenamePath(self):
        return self.dataload_hive_json_filenamePath

    # 传入datestring值，用于拼接csv文件名
    def set_datestring(self, datestring):
        self.datestring = datestring

    def get_datestring(self):
        return self.datestring

    def set_bigdata_name(self, bigdata_name):
        self.bigdata_name = bigdata_name

    def get_bigdata_name(self):
        return self.bigdata_name

    # 返回表结构字段列表
    def set_table_fields_list(self, table_fields_list):
        self.table_fields_list = table_fields_list

    def get_table_fields_list(self):
        return self.table_fields_list

    def set_dataload_time(self, dataload_time):
        self.dataload_time = dataload_time

    def get_dataload_time(self):
        return self.dataload_time

    # 分析字段列表
    def analyse_table_fields(self):
        filepath = self.table_fields_jsonpath
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                json_cont = json.load(file)
                table_fields_list = json_cont["fields"]
                self.set_table_fields_list(table_fields_list)
        except Exception as e:
            print("analyse_table_fields: ", e)

    # 获取json数据列表，补全json格式文件中不存在的字段
    def read_jsonfile(self):
        file = self.dataload_hive_json_filenamePath
        try:
            datestring = self.get_datestring()
            jsonfile_name_path = BASE_DIR + "/dataload/csv/%s/%s_%s.json" % (self.bigdata_name,
                                                                             datestring,
                                                                             self.bigdata_name)
            # 传参
            self.set_csv_file(jsonfile_name_path)

            with open(file, 'r', encoding='utf-8') as file:
                jsonfile_conts = file.readlines()
                for jsonfile_cont in jsonfile_conts:
                    json_cont = json.loads(jsonfile_cont)
                    jsonfile_keys = json_cont.keys()
                    table_fields_list = self.get_table_fields_list()
                    fields_list_dic = "{" + '"' + '":"NULL","'.join(table_fields_list) + '":"NULL"' + "}\n"
                    # fields_list_dic = dict(fields_list_dic)
                    # 需要打印确认格式
                    """
                    fields_list_dic:  
                    {'"date_st":"NULL",time_st":"NULL",bigdata":"NULL",component_service":"NULL",
                    hdfs_usage":"NULL",ip":"NULL",hostname":"NULL",heap_usage":"NULL",
                    component_service_status":"NULL",hdfs_added":"NULL",namenode_ha_status":"NULL",
                    hdfs_healthy":"NULL",alive_nodes":"NULL",dead_nodes":"NULL",volume_failure":"NULL",
                    gctime":"NULL",client_num":"NULL",rm_core_total":"NULL",rm_core_used":"NULL",
                    rm_core_avalable":"NULL",rm_mem_total":"NULL",rm_mem_use":"NULL",rm_mem_available":"NULL",
                    Apprunning":"NULL",AppFailed":"NULL",AppSubmitted":"NULL",AppSubmitted_perday":"NULL",
                    AppPending":"NULL",NodeManager_healthy":"NULL",yarn_nospace":"NULL",Apppending_longten":"NULL",
                    rootqueue_usage_percent"'}
                    """
                    print("fields_list_dic: ", fields_list_dic)
                    print("fields_list_dic type: ", type(fields_list_dic))
                    fields_list_dic = json.loads(fields_list_dic)
                    print("fields_list_dic type: ", type(fields_list_dic))

                    print("json_cont: ", json_cont)
                    print("json_cont type", type(json_cont))

                    for key in jsonfile_keys:
                        value = json_cont[key]
                        # fields_list_dic[key] = jsonfile_cont[key]
                        fields_list_dic["%s" % key] = value

                    # 将生成的完整的json数据写入到json文件中
                    # jsonfile_name_path:  /export/monitor_xjjk/dataload/csv/None/None_None.json
                    # print(jsonfile_name_path)
                    csv_filepath = self.get_csv_file()
                    with open(csv_filepath, 'a', encoding='utf-8') as jsonfile:
                        jsonfile.write(str(fields_list_dic).replace("'", "\"") + '\n')
                        jsonfile.close()
                file.close()

            # 将生成的全部字段的json文件的值写入到csv文件中，逗号分隔
            # 每次检查生成的数据为当天日期的文件
            # csv_file = self.csv_file
            csv_file = self.get_csv_file().strip().replace(".json", ".csv")

            with open(csv_file, 'a', encoding='utf-8') as f:
                # jsonfile_name_path = jsonfile_name_path
                csv_writer = csv.writer(f)
                with open(jsonfile_name_path, 'r', encoding='utf-8') as file:
                    conts = file.readlines()
                    for cont in conts:
                        json_cont = json.loads(cont)
                        csv_writer.writerow(json_cont.values())
                    # csv_writer.close()
                    file.close()
                f.close()

            # 追加写入数据到csv目录下日期下的日期.csv文件
            # 创建时间日期文件夹
            path = BASE_DIR + "/csv/%s" % datestring.strip()[0:8]
            lastdate = str(int(datestring.strip() - 1))[0:8]
            lastdatepath = BASE_DIR + "/csv/%s" % lastdate
            if os.path.exists(path):
                pass
            else:
                os.mkdir(path)

            # 如果第一次运行前一天的数据文件不存在，啧复制当前的当天数据文件到前一天日期中
            if not os.path.exists(lastdatepath):
                os.mkdir(lastdatepath)
                shutil.copyfile(path, lastdatepath)

            # 将数据追加写入到路径 /csv/$(date +%Y%m%f)/$(date +%Y%m%d).csv； 当天的日期
            final_csv_all_path = path + "/%s.csv" % datestring.strip()[0:8]
            self.set_final_csv_filepath(final_csv_all_path)
            csv_filepath = self.get_final_csv_filepath()
            with open(csv_filepath, 'a', encoding='utf-8') as ff:
                csv_writer = csv.writer(ff)
                with open(jsonfile_name_path, 'r', encoding='utf-8') as file:
                    conts = file.readlines()
                    for cont in conts:
                        json_cont = json.loads(cont)
                        csv_writer.writerow(json_cont.values())
                    # csv_writer.close()
                    file.close()
                ff.close()

            # 设置变量，判断时分来进行导入数据
            dataload_time = self.get_dataload_time()
            datestring = self.get_datestring()
            if str(dataload_time).strip().replace(":", "") == str(datestring).strip()[8:-2]:
                print("datahiveWriter: ", self.dataload_time.strip().replace(":", ""), " ",
                      self.datestring.strip()[8:-2])
                hiveserver2_ip = self.get_hiveserver2_ip()
                hiveserver2_port = self.get_hiveserver2_port()
                print("datahiveWriter.py hiveserver2_ip： ", hiveserver2_ip)
                print("datahiveWriter.py hiveserver2_port： ", hiveserver2_port)

                """
                传参数到hiveUtils,进行kerberos认证
                来源路线：hivePD --> datahivewriter --> dataLoad -->hiveUtils
                """
                krb5conf = self.get_krb5conf()
                self.dataloader.set_krb5conf(krb5conf)

                client_keytab = self.get_client_keytab()
                self.dataloader.set_client_keytab(client_keytab)

                name = self.get_cluster_name()
                self.dataloader.set_cluster_name(name)

                client_keytab_principle = self.get_client_keytab_principle()
                self.dataloader.set_client_keytab_principle(client_keytab_principle)

                self.dataloader.set_hiveserver2_ip(hiveserver2_ip)
                self.dataloader.set_hiveserver2_port(hiveserver2_port)
                # 传参：/csv/$(date +%y%m%d)/${date %y%m%d}.csv
                csv_file_path = self.get_csv_file()
                self.dataloader.set_csv_filepath(csv_file_path)
                self.dataloader.set_cmd(cmd=self.cmd)
                self.dataloader.loaddata_main()

        except Exception as e:
            print("read_jsonfile: ", e)
            # print("read_jsonfile: ", e)

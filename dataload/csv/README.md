# csv格式文件导入HIVE数据库

```text
存放导入hive数据的数据文件

csv文件格式
逗号分隔

# 建表语句
create table monitorxjjk(
date_st string comment '6位日期数值，例:20230712 指2023年7月12日',
time_st string comment '6位时间数值，例:104400 值10点44分00秒',
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

```

此处存放重构的json数据格式以及重构后生成的scv文件(/dataload/csv);
生成完成后，在每天的00:00时刻（可以定制时间），拷贝具有前一天所有数据的csv文件拷贝到目/csv/
然后将数据文件csv 文件 导入到hive的数据库表中;


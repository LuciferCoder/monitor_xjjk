# monitor_xjjk
monitor of bigdata cluster with kerberos in version hadoop-3.2.2

适用于hadoop3.2.2\hive3.1.0版本的巡检脚本

一、Hadoop巡检分为两部分：

（1）hdfs
```text
hdfs巡检指标：
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
```

（2）yarn
```text
yar巡检指标：
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
```

二、hive-3.1.0
```text
巡检指标
（1）组件服务的状态（大数据端口探活告警）
（2）Hive metastore的GC时间
（3）Hive metastore的内存使用
（4）Hive server2的内存使用
（5）Hiveserver2的GC时间
（6）Hvieserver2的连接客户端数量
```

三、指标入库，mysql数据库初始化
```mysql
# 建表语句
create table monitorxjjk(
	`id` int(250) PRIMARY KEY AUTO_INCREMENT,
	`date` varchar(50) NOT NULL,
	`time` varchar(50) NOT NULL,
	`bigdata_component` varchar(50) DEFAULT NULL,
	`component_service` varchar(50) DEFAULT NULL,
	`hdfs_usage` varchar(50) DEFAULT NULL,
	`ip` varchar(50) DEFAULT NULL,
	`hostname` varchar(250) DEFAULT NULL,
	`heap_usage` varchar(50) DEFAULT NULL,
	`component_service_status` int(20) DEFAULT NULL,
	`hdfs_added` varchar(50) DEFAULT NULL,
	`namenode_ha_status` int(20) DEFAULT NULL,
	`hdfs_healthy` int(20) DEFAULT NULL,
	`alive_nodes` int(250)  DEFAULT NULL,
	`dead_nodes` int(250)  DEFAULT NULL,
	`volume_failure` int(250) DEFAULT NULL,
	`gctime` int(250) DEFAULT NULL,
	`client_num` int(250) DEFAULT NULL,
	`rm_core_total` int(250) DEFAULT NULL,
	`rm_core_used` int(250) DEFAULT NULL,
	`rm_core_avalable` int(250) DEFAULT NULL,
	`rm_mem_total` int(250) DEFAULT NULL,
	`rm_mem_use` int(250) DEFAULT NULL,
	`rm_mem_available` int(250) DEFAULT NULL,
	`Apprunning` int(250) DEFAULT NULL,
	`AppFailed` int(250) DEFAULT NULL,
	`AppSubmitted` int(250) DEFAULT NULL,
	`AppSubmitted_perday` int(250) DEFAULT NULL,
	`AppPending` int(250) DEFAULT NULL,
	`NodeManager_healthy` int(250) DEFAULT NULL,
	`yarn_nospace` int(250) DEFAULT NULL,
	`Apppending_longten` int(250) DEFAULT NULL,
	`rootqueue_usage_percent` varchar(50) DEFAULT NULL;
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

# 创建索引
ALTER TABLE `monitorxjjk` ADD INDEX id (`id`);
ALTER TABLE `monitorxjjk` ADD INDEX date (`date`);
ALTER TABLE `monitorxjjk` ADD INDEX time (`time`);
ALTER TABLE `monitorxjjk` ADD INDEX bigdata_component (`bigdata_component`);
ALTER TABLE `monitorxjjk` ADD INDEX component_service (`component_service`);
ALTER TABLE `monitorxjjk` ADD INDEX ip (`ip`);
```

四、配置文件修改
```text
├─dataload
│  └─datasourceManager
├─hbase
├─hdfs
├─hive
├─yarn
└─__pycache__
```



五、hive指标入库：
```sql
# hive建表语句
# 建表语句
create table tmp.monitorxjjk(
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

# 定时任务
```
30 4 * * * /usr/bin/python3 /export/monitor_xjjk/bin/namenode.py use_crontab="true"
10 * * * * /usr/bin/python3 /export/monitor_xjjk/bin/resourcemanager.py use_crontab="true"
10 * * * * /usr/bin/python3 /export/monitor_xjjk/bin/hivePD.py use_crontab="true"
```



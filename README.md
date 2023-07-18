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

```text
（1）组件服务的状态（大数据端口探活告警）
（2）Hive metastore的GC时间
（3）Hive metastore的内存使用
（4）Hive server2的内存使用
（5）Hiveserver2的GC时间
（6）Hvieserver2的连接客户端数量

```
![img.png](img.png)

```shell script
拟定使用脚本方式
#定时任务执行方式：
python3 resourceManager.py use_crontab="true"
python3 resourceManager.py use_crontab=true


#手动执行方式：
python3 resourceManager.py use_crontab="false"
python3 resourceManager.py use_crontab=false
python3 resourceManager.py
```

```shell script
# 定时任务 每10分钟运行一次
*/10 * * * * /usr/bin/python3 /export/monitor_xjjk/bin/resourceManager.py use_crontab="true"
```

追加配置信息到 hive-site.xml中
重启hiveserver的服务，开启hive的jmx监控
```text
<!-- 开启 metrix -->
<property>
    <name>hive.metastore.metrics.enabled</name>
    <value>true</value>
</property>

<property>
    <name>hive.metastore.metrics.enabled</name>
    <value>hive.server2.metrics.enabled</value>
</property>


```

```mysql
# 建表语句
create table monitorxjjk(
	`id` int(250) PRIMARY KEY AUTO_INCREMENT,
	`date` varchar(50) NOT NULL,
	`time` varchar(50) NOT NULL,
	`bigdata_component` varchar(50) NOT NULL,
	`component_service` varchar(50) NOT NULL,
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
	`Apppending_longten` int(250) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

# 添加普通索引
ALTER TABLE `monitorxjjk` ADD INDEX id (`id`);
ALTER TABLE `monitorxjjk` ADD INDEX date (`date`);
ALTER TABLE `monitorxjjk` ADD INDEX time (`time`);
ALTER TABLE `monitorxjjk` ADD INDEX bigdata_component (`bigdata_component`);
ALTER TABLE `monitorxjjk` ADD INDEX component_service (`component_service`);
ALTER TABLE `monitorxjjk` ADD INDEX ip (`ip`);

ALTER TABLE `monitorxjjk` DROP INDEX id ;
ALTER TABLE `monitorxjjk` DROP INDEX date;
ALTER TABLE `monitorxjjk` DROP INDEX time ;
ALTER TABLE `monitorxjjk` DROP INDEX bigdata_component ;
ALTER TABLE `monitorxjjk` DROP INDEX component_service ;
ALTER TABLE `monitorxjjk` DROP INDEX ip ;

```
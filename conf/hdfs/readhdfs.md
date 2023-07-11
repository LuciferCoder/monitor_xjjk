```text
hdfs 巡检需求
五、hdfs
（1）HDFS使用率超过70%
（2）HDFS使用率超过85%
（3）DFSNameNode堆内存使用率超过70%
（4）HDFSNameNode堆内存使用率超过90%
（5）DataNode is down
（6）NameNode is down
（7）HDFS日增数据量
（8）Namenode HA的状态
（9）HDFS健康检查
（10）集群节点数(Live Nodes)
（11）集群节点数(Dead Nodes )
（12）集群DN节点磁盘(Total Datanode Volume Failures）

纠正检查顺序：
（6）NameNode is down
（8）Namenode HA的状态
（3）DFSNameNode堆内存使用率超过70%
（4）HDFSNameNode堆内存使用率超过90%

（5）DataNode is down
（10）集群节点数(Live Nodes)
（11）集群节点数(Dead Nodes )
（12）集群DN节点磁盘(Total Datanode Volume Failures）
（9）HDFS健康检查
（1）HDFS使用率超过70%
（2）HDFS使用率超过85%
（7）HDFS日增数据量
```

```shell script
拟定使用脚本方式
#定时任务执行方式：
python3 hdfs.py use_crontab="true"
python3 hdfs.py use_crontab=true


#手动执行方式：
python3 hdfs.py use_crontab="false"
python3 hdfs.py use_crontab=false
python3 hdfs.py
```

```shell script
# 定时任务
30 4 * * * /usr/bin/python3 /export/monitor_xjjk/bin/hdfs.py use_crontab="true"
```
```text
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
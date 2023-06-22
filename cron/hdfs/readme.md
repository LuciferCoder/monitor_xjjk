```text
当前文件夹为存放日增长量，当前日期的hdfs使用量

文件格式：年月日时分秒__crontab_hdfs_capcity.json
20230621151619_crontab_hdfs_capcity.json
{growuped: "",hdfscap: ""}

#特例：
当检查日期的前一天文件不存在的情况下写入当前日期的文件两个值当前获取的hdfs使用量值
{growuped: "GB",hdfscap: "0GB"}
```
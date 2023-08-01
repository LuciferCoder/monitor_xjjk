from pyhive import hive
import os
import sys
import krbticket
# import hdfs
from pyhdfs import HdfsClient

from datetime import datetime, timedelta



# 设置本地路径
'''设置路径,添加本地环境路径 项目路径'''
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

"""
工具主要目的是上传文件到hdfs目录，dataload完成之后删除文件
"""


class HDFSFILETUTILS(object):

    # 初始化参数
    def __init__(self):
        # 文件路径参数
        # hdfs,此处应该是 hdfs
        self.hdfsport = None
        # hdfs端口
        self.hdfshost = None
        self.bigdata_comment_name = None
        # 此处应该是 hdfs/ocean
        self.client_keytab_principle = None
        # /export/kerberos/1/hdfs.keytab
        self.client_keytab = None
        # /etc/krb5.conf
        self.krb5conf = None
        self.csv_filepath = None
        self.hdfs_base_dir = "/tmp"
        self.date_date = None
        # hdfs上传文件路径：/tmp/${date}/${filename}
        self.hdfs_client = None

    def set_krb5conf(self, krb5conf):
        self.krb5conf = krb5conf

    def get_krb5conf(self):
        return self.krb5conf

    def set_client_keytab(self, client_keytab):
        self.client_keytab = client_keytab

    def get_client_keytab(self):
        return self.client_keytab

    def set_client_keytab_principle(self, client_keytab_principle):
        self.client_keytab_principle = client_keytab_principle

    def get_client_keytab_principle(self):
        return self.client_keytab_principle

    # hdfs client
    def set_hdfs_client(self):
        # client = HdfsClient(hosts='localhost:50070')
        host = self.get_hdfshost()
        port = self.get_hdfsport()
        client = HdfsClient(hosts='%s:%s' % (host, port))
        self.hdfs_client = client

    # 返回 hdfs client
    def get_hdfs_client(self):
        return self.hdfs_client

    # 日期参数传入
    def set_date(self, date):
        self.date_date = date

    # 日期参数返回
    def get_date_date(self):
        return self.date_date

    # 返回csv本地文件参数
    def get_csv_filepath(self):
        return self.csv_filepath

    # 传入参数，传入参数后调用参数必须使用返回参数函数进行调用
    def set_csv_filepath(self, csv_filepath):
        self.csv_filepath = csv_filepath

    # 文件按上传
    def hdfsput(self):
        client = self.get_hdfs_client()
        # client = HdfsClient(hosts="localhost:50070")
        datestr = str(self.get_date_date())[0:8]
        hdfs_base_dir = self.hdfs_base_dir
        dest_dir = "/".join([hdfs_base_dir, datestr])
        curfilepath = self.get_csv_filepath()
        client.copy_from_local(localsrc=curfilepath, dest=dest_dir)

    # 文件下载，目前用不到，暂时不编辑
    def hfdsget(self):
        pass

    # hdfs文件删除
    def hdfsdel(self):
        client = self.get_hdfs_client()
        # client = HdfsClient(hosts="localhost:50070")
        datestr = str(self.get_date_date())[0:8]
        hdfs_base_dir = self.hdfs_base_dir
        dest_dir = "/".join([hdfs_base_dir, datestr])
        curfilepath = self.get_csv_filepath()
        filename = os.path.basename(curfilepath)
        path = dest_dir + "/" + filename
        client.delete(path=path)

    # 创建文件夹目录
    def hdfsmkdir(self):
        client = self.get_hdfs_client()
        # client = HdfsClient(hosts="localhost:50070")
        datestr = str(self.get_date_date())[0:8]
        hdfs_base_dir = self.hdfs_base_dir
        dest_dir = "/".join([hdfs_base_dir, datestr])
        # curfilepath = self.get_csv_filepath()
        # filename = os.path.basename(curfilepath)
        # path = dest_dir + "/" + filename
        client.mkdirs(dest_dir)

    # kerberos认证
    # 如果使用了kerberos，调用此方法
    def krb5init(self):
        krbconf = self.get_krb5conf()
        # keytab_file = self.client_keytab
        keytab_file = self.get_client_keytab()
        # principle = self.client_keytab_principle
        principle = self.get_client_keytab_principle()
        os.environ['KRB5CCNAME'] = os.path.join(BASE_DIR, f'keytab/krb5cc_%s' % self.bigdata_comment_name)
        kconfig = krbticket.KrbConfig(principal=principle, keytab=keytab_file)
        krbticket.KrbCommand.kinit(kconfig)
        # cont = krbticket.KrbTicket.get(keytab=keytab_file,principal=principle)

    def get_hdfshost(self):
        return self.hdfshost

    def set_hdfshost(self, hdfs_host):
        self.hdfshost = hdfs_host

    def get_hdfsport(self):
        return self.hdfsport

    def set_hdfsport(self, hdfsport):
        self.hdfsport = hdfsport


def main():
    # 初始化
    hdfsfileutiler = HDFSFILETUTILS()
    datenow = datetime.now()
    hdfsfileutiler.set_date("")
    # datenowstring = datenow.strftime("%Y%m%d%H%M%S")
    datenowdate = datenow.strftime("%Y%m%d")
    # datenowtime = datenow.strftime("%H%M%S")
    hdfsfileutiler.set_date(datenowdate)
    hdfsfileutiler.set_hdfshost("10.0.0.47")
    hdfsfileutiler.set_hdfsport("50470")
    path = "/export/monitor_xjjk/csv/20230726/20230726.csv"
    hdfsfileutiler.set_csv_filepath(path)
    hdfsfileutiler.set_client_keytab("/export/kerberos/1/hdfs.keytab")
    hdfsfileutiler.set_client_keytab_principle("hdfs/ocean@JD.COM")
    hdfsfileutiler.set_krb5conf("/etc/krb5.conf")
    hdfsfileutiler.krb5init()
    hdfsfileutiler.set_hdfs_client()

    hdfsfileutiler.hdfsmkdir()
    hdfsfileutiler.hdfsput()


if __name__ == '__main__':
    main()


from hdfs.ext.kerberos import KerberosClient
hdfs_client = KerberosClient('https://10.0.0.47:50470;https://10.0.0.45:50470')
hdfs_client.status("/")


"""
测试成功
"""
from hdfs.ext.kerberos import KerberosClient
import requests
import logging

logging.basicConfig(level=logging.DEBUG)

session = requests.Session()
session.verify = False

client = KerberosClient(url='https://10.0.0.45:50470', session=session,mutual_auth='REQUIRED',principal='hdfs/ocean@JD.COM')
client.acl_status()
client.upload(hdfs_path="/tmp/monitor/", local_path="/export/")
client.set_permission()

print(client.list('/'))
from pyhive import hive
import os
import sys
import krbticket
import pyhdfs

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
        self.csv_filepath = None


    # 返回参数
    def get_csv_filepath(self):
        return self.csv_filepath

    # 传入参数，传入参数后调用参数必须使用返回参数函数进行调用
    def set_csv_filepath(self, csv_filepath):
        self.csv_filepath = csv_filepath

    # 文件按上传
    def hdfsput(self):
        pass

    # 文件下载，可能用不到
    def hfdsget(self):
        pass

    # hdfs文件删除
    def hdfsdel(self):
        pass

    # 创建文件夹目录
    def hdfsmkdir(self):
        pass

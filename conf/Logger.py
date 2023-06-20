# coding:utf-8
import logging
import time
import re
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler

# 日志打印格式
log_fmt = '%(asctime)s\tFile \"%(filename)s\",line %(lineno)s\t%(levelname)s: %(message)s'
formatter = logging.Formatter(log_fmt)


class Log_module():

    def __init__(self, logname):
        # logging.basicConfig()
        self.logname = logname

    def file_handler_timed_rotate(self, filename, when='d', interval=1, backupCount=7, log_level=logging.INFO,
                                  suffix="%Y-%m-%d.log"):
        logname = self.logname
        file_handler_timed_rotate = logging.getLogger(logname)
        file_handler_timed_rotate.setLevel(log_level)

        # 定义一个10秒换一次log文件的handler，保留5个旧log文件
        # filehandler = logging.handlers.TimedRotatingFileHandler(filename="logs/myapp.log", when='S', interval=10, backupCount=5)
        filehandler = logging.handlers.TimedRotatingFileHandler(filename=filename, when=when, interval=interval,
                                                                backupCount=backupCount)
        filehandler.setFormatter(formatter)
        # 设置后缀名称，跟strftime的格式一样
        filehandler.suffix = suffix
        # filehandler.extMatch = extMatch
        # filehandler.suffix = "%Y-%m-%d.log"
        # filehandler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
        file_handler_timed_rotate.addHandler(filehandler)

        return file_handler_timed_rotate

    def file_handler_size_rotate(self, filename, maxBytes, backupCount, mode='a'):
        # file_handler_size_rotate = logging.getLogger(logname)
        file_handler_size_rotate = logging.getLogger(self.logname)
        file_handler_size_rotate.setLevel(logging.INFO)

        # filehandler = logging.handlers.RotatingFileHandler(filename="logs/file_handler_size_rotate.log",mode='a',maxBytes=10485760,backupCount=5)
        filehandler = logging.handlers.RotatingFileHandler(filename=filename, maxBytes=maxBytes,
                                                           backupCount=backupCount, mode=mode)
        filehandler.setFormatter(formatter)
        file_handler_size_rotate.addHandler(filehandler)
        return file_handler_size_rotate

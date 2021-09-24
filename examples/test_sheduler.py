#! encoding: utf-8
from scheduler import Scheduler
import logging
import time

logging.basicConfig(level=logging.NOTSET)


def callback(data):
    print(data)


servers = ['wang']
s = Scheduler(servers=servers, port=12345, pid_file='/tmp/py-scheduler.pid', callback_func=callback)


@s.when(job_name='job1', job_info='* * * * *')
def job1():
    time.sleep(40)
    print('complete job1')


s.start(backgroud=False)

# Python 定时任务

## 定时任务查看客户端

http://l-pyscheduler.ops.cn5:8083


## 用法

```
from scheduler import Scheduler
import time
import logging
logging.basicConfig(level=logging.NOTSET)

def callback(data):
    print(data)

servers = ['wang','qunar-opsdev-wb-archlinux']
s = Scheduler(servers=servers, port=12345, pid_file='/tmp/py-scheduler.pid', callback_func=callback)


@s.when(job_name='job', job_info='* * * * *')
def run_every_minute():
    print('complete job')

s.start(background=False)

 

```

## 定时任务相关http接口

#### 查看任务列表

http://localhost:12345/info

#### 开启关闭任务

http://localhost:12345/job/start?job_id=xxxx
http://localhost:12345/job/stop?job_id=xxxx

#### 手动执行任务

http://localhost:12345/job/manual_do?job_id=xxxx


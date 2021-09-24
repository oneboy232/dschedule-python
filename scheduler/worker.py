#! encoding: utf-8

from scheduler.callback import Callback
from scheduler.cron import Cron
from scheduler.lock import Lock
from scheduler.topology import Topology
import time
import hashlib
import threading
import logging

from scheduler.util import get_now_str, get_pid


class Scheduler(object):

    def __init__(self, servers, port, callback_func=None, pid_file=None):
        self.job_map = {}
        self.servers = servers
        self.port = port
        self.lock = Lock(pid_file)
        self.callback = Callback(callback_func)

    def load(self, modules):
        for module in modules:
            __import__(module, None)

    def start(self, backgroud=True):
        if self.lock.acquire():
            logging.info('获取文件夹锁成功，启动定时任务。')
        else:
            logging.info('获取文件锁失败，本进程不执行定时任务。')
            return
        for job_id, job in self.job_map.items():
            logging.info(
                'job_id:{}\tjob_name:{}\tjob_info:{}\tcallback:{}'.format(job_id, job['job_name'], job['job_info'],
                                                                          job['callback']))
        self.run(backgroud)

    def when(self, job_name, job_info, callback=False):
        def decorator(func):
            hash = hashlib.md5()
            job_str = job_name + '_' + func.__code__.co_filename.split('/')[-1] + '_' + func.__name__
            hash.update(job_str.encode('utf-8'))
            job_id = hash.hexdigest()
            self.job_map[job_id] = {
                'job_id': job_id,
                'job_name': job_name,
                'job_info': job_info,
                'job_func': func,
                'cron': Cron(job_info),
                'state': 'on',
                'callback': callback,
                'last_state': 0,
                'last_time': '',
                'manual_do': False,
                'job_path': func.__code__.co_filename
            }
            return func

        return decorator

    def run(self, backgroud=True):
        # 启动网络维护线程
        self.topology = Topology(self.servers, self.port, self.job_map)
        self.topology.setDaemon(True)
        self.topology.start()
        self.background_thread = BackgroundThread(self.topology, self.job_map, self.callback)
        if backgroud:
            # 启动后台线程
            self.background_thread.setDaemon(True)
            self.background_thread.start()
        else:
            self.background_thread.run()


class BackgroundThread(threading.Thread):
    def __init__(self, topology, job_map, callback):
        super(BackgroundThread, self).__init__()
        self.topology = topology
        self.job_map = job_map
        self.callback = callback
        self.is_running = True

    def run(self):
        logging.info('pid: {},定时任务启动'.format(get_pid()))
        while self.is_running:
            self.check_job()
            time.sleep(1)
        logging.info('任务维护线程退出')

    def stop(self):
        self.is_running = False
        self.topology.stop()

    def check_job(self):
        time_list = Cron.get_current_time()
        for job_id, job in self.job_map.items():
            if job['manual_do'] and self.topology.need_execute(job_id):
                JobThread(job, self.callback, self.topology).start()
                job['manual_do'] = False
                self.topology.jobmapmsg()
            if job['cron'].times_up(time_list) and self.topology.need_execute(job_id):
                JobThread(job, self.callback, self.topology).start()


class JobThread(threading.Thread):

    def __init__(self, job, callback, topology):
        super(JobThread, self).__init__()
        self.job = job
        self.callback = callback
        self.topology = topology

    def run(self):
        self.callback.call(self.job, 'start')
        try:
            # 执行任务
            logging.info('pid: {}, job_id {} start'.format(get_pid(), self.job['job_id']))
            self.job['last_state'] = 1
            self.job['last_time'] = get_now_str()
            self.topology.jobmapmsg()
            self.job['job_func']()
        except Exception as e:
            logging.exception('job_id {} error {}'.format(self.job['job_id'], e))
            self.job['last_state'] = 3
            self.job['last_time'] = get_now_str()
            self.topology.jobmapmsg()
            self.callback.call(self.job, 'error', e)
            return
        self.job['last_state'] = 2
        self.job['last_time'] = get_now_str()
        self.topology.jobmapmsg()
        logging.info('job_id {} complete'.format(self.job['job_id']))
        self.callback.call(self.job, 'end')

#! encoding: utf-8

import fcntl
import os


class Lock(object):

    def __init__(self, file_name):
        self.file_name = file_name
        if self.file_name is None:
            self.file_name = '/tmp/py-sheduler.pid'
        if not os.path.exists(self.file_name):
            with open(self.file_name, 'w') as file:
                file.write('lock')
        self.file_lock = None

    def acquire(self):
        try:
            self.file_lock = open(self.file_name, 'w')
            fcntl.flock(self.file_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except:
            return False
        return True

#! encoding: utf-8

import logging


class Callback(object):

    def __init__(self, callback_func):
        self.callback_func = callback_func

    def call(self, job, state, msg=''):
        if job['callback']:
            self.call_func(job, state, msg)

    def call_func(self, job, state, msg):
        if self.callback_func is None:
            return
        try:
            self.callback_func({'job_id': job['job_id'], 'state': state, 'msg': '{} {}'.format(job['job_name'], msg)})
        except Exception as e:
            logging.error('执行函数回调错误{}'.format(e))

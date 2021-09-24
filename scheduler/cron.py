#! encoding: utf-8
import re, time


class Cron(object):

    def __init__(self, job_info):
        self.job_info = job_info
        self.parse_info = parse_cron(self.job_info)
        self.last_time = self.get_current_time()

    #  询问是否可以执行
    def times_up(self, time_list):
        cur = time_list
        has_execute = True
        if self.last_time:
            for i in range(len(time_list) - 1):
                if time_list[i] != self.last_time[i]:
                    has_execute = False
                    break
        else:
            has_execute = False
        if has_execute:
            return False
        for i in range(len(type_list)):
            if cur[i] not in self.parse_info[type_list[i]]:
                return False
        self.last_time = time_list
        return True

    @staticmethod
    def get_current_time():
        st_time = time.localtime(int(time.time()))
        return [st_time.tm_min, st_time.tm_hour, st_time.tm_mday, st_time.tm_mon, st_time.tm_wday, st_time.tm_sec]


type_range = {
    'min': (0, 59),
    'hour': (0, 23),
    'day': (1, 31),
    'month': (1, 12),
    'week': (0, 6)
}

type_list = ['min', 'hour', 'day', 'month', 'week']


# 将时刻转换为时间列表
# 比如 *分钟 转 [0,1,2,3,4,5,6,7...59]
def time_to_list(conf, type):
    time_range = type_range[type]
    result_list = []
    # 纯数字
    if re.match('^[0-9]+$', conf):
        if time_range[0] <= int(conf) <= time_range[1]:
            result_list.append(int(conf))
            return result_list

    # 纯星号
    if conf == '*':
        for i in range(time_range[1] - time_range[0] + 1):
            result_list.append(i + time_range[0])
        return result_list

    # 星号和数字组合
    if re.match('^\*\/[0-9]+$', conf):
        confs = conf.split('/')
        step = int(confs[1])
        if step < 1:
            return []
        tmp = int(confs[1])
        result_list.append(time_range[0])
        while tmp <= time_range[1]:
            result_list.append(tmp)
            tmp = tmp + step
        return result_list
    return result_list


def parse_cron(cron_str):
    cron_list = [c.strip() for c in cron_str.split(' ') if c != '']
    result = {}
    for i in range(len(cron_list)):
        result[type_list[i]] = time_to_list(cron_list[i], type_list[i])
        if len(result[type_list[i]]) == 0:
            raise RuntimeError('cron格式错误!')
    return result


if __name__ == '__main__':
    cron = Cron('* * * * *')
    print(cron.times_up())

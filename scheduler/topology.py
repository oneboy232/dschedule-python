#! encoding: utf-8

import threading

from scheduler.util import get_pid

try:
    import socketserver
except ImportError:
    import SocketServer as socketserver
import time
import hashlib
import socket
import json
import logging
import sys


def to_str(data):
    if sys.version[0] == '3':
        return str(data, encoding='utf-8')
    else:
        return str(data)


def to_bytes(str):
    if sys.version[0] == '3':
        return bytes(str, encoding='utf-8')
    else:
        return bytes(str)


# 端口复用TcpServer
class ReuseTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, RequestHandlerClass):
        socketserver.ThreadingTCPServer.__init__(self, server_address, RequestHandlerClass)


# 维护网络拓扑结构
class Topology(threading.Thread):
    servers = []
    topology = []
    hostname = ''
    job_map = None
    port = 6666
    check_count = 0
    check_dump_interval = 30
    buf_size = 1024

    def __init__(self, servers, port, job_map):
        super(Topology, self).__init__()
        Topology.servers = servers
        Topology.servers.sort()
        Topology.topology = list(servers)
        Topology.hostname = socket.gethostname()
        Topology.port = port
        Topology.job_map = job_map
        self.srv = ReuseTCPServer(
            ("0.0.0.0", self.port), self.TcpHandler)
        self.is_running = True
        logging.info(Topology.topology)

    class TcpHandler(socketserver.BaseRequestHandler):

        def transform_job_server(self):

            job_list = []
            for job_id, job in Topology.job_map.items():
                job_list.append({
                    'job_id': job_id,
                    'job_name': job.get('job_name'),
                    'state': job.get('state'),
                    'job_info': job.get('job_info'),
                    'job_server': Topology.get_host(job_id),
                    'last_state': job.get('last_state'),
                    'last_time': job.get('last_time'),
                    'manual_do': job.get('manual_do'),
                    'job_path': job.get('job_path')
                })

            return {'job_map': job_list, 'server_map': Topology.topology}

        def handle_http(self, data):
            request = data.split(' ')
            url = request[1].split('?')

            response_content = \
                'HTTP/1.1 200 ok\r\nContent-Type: text/html\r\nAccess-Control-Allow-Origin:*' \
                '\r\nAccess-Control-Allow-Headers:X-Requested-With,accept, origin, content-type\r\n\r\n'

            if url[0] == '/info':
                response_content += json.dumps(self.transform_job_server())

            elif url[0] == '/job/start':
                job_id = url[1].split('=')[1]

                if Topology.job_map.get(job_id):
                    Topology.job_map.get(job_id)['state'] = 'on'
                    Topology.broadcast()
                    response_content += json.dumps(self.transform_job_server())
                    logging.info('{} start'.format(job_id))
                else:
                    response_content += 'jid not found'

            elif url[0] == '/job/stop':
                job_id = url[1].split('=')[1]

                if Topology.job_map.get(job_id):
                    Topology.job_map.get(job_id)['state'] = 'off'
                    Topology.broadcast()
                    response_content += json.dumps(self.transform_job_server())
                    logging.info('{} stop'.format(job_id))
                else:
                    response_content += 'jid not found'

            elif url[0] == '/job/manual_do':
                job_id = url[1].split('=')[1]

                if Topology.job_map.get(job_id):
                    Topology.job_map.get(job_id)['manual_do'] = True
                    Topology.broadcast()
                    response_content += json.dumps(self.transform_job_server())
                    logging.info('{} manual_do'.format(job_id))
                else:
                    response_content += 'jid not found'

            else:
                response_content += 'OK'
            self.request.sendall(to_bytes(response_content))

        def handle_msg(self, data):
            jdata = json.loads(data)

            if jdata.get('check'):
                if jdata.get('check') in Topology.topology:
                    resp = {'check_resp': []}
                else:
                    resp = {'check_resp': Topology.topology}

                try:
                    jresp = json.dumps(resp) + '\r\n\r\n'
                    self.request.sendall(to_bytes(jresp))
                    if jdata.get('check') not in Topology.topology:
                        Topology.topology.append(jdata.get('check'))

                except Exception as e:
                    logging.error('response check error:{}'.format(e))

            if jdata.get('heartbeat'):
                if jdata.get('heartbeat') in Topology.topology:
                    return
                else:
                    Topology.topology.append(jdata.get('heartbeat'))
                    logging.info('拓扑更新 {}'.format(Topology.topology))
                    Topology.broadcast()

            if jdata.get('broadcast'):
                for k, v in jdata.get('broadcast').get('server_map').items():
                    if v:
                        if k not in Topology.topology:
                            Topology.topology.append(k)
                            logging.info('拓扑更新 {}'.format(Topology.topology))
                    else:
                        if k in Topology.topology:
                            Topology.topology.remove(k)
                            logging.info('拓扑更新 {}'.format(Topology.topology))

                for k, v in jdata.get('broadcast').get('job_map').items():
                    if Topology.job_map.get(k):
                        Topology.job_map.get(k)['state'] = v.get('state')
                        Topology.job_map.get(k)['last_time'] = v.get('last_time')
                        Topology.job_map.get(k)['last_state'] = v.get('last_state')
                        Topology.job_map.get(k)['manual_do'] = v.get('manual_do')
                    else:
                        logging.info('broadcast 未发现任务jid {}'.format(k))

            if jdata.get('jobmapmsg'):

                for k, v in jdata.get('jobmapmsg').get('job_map').items():
                    if Topology.job_map.get(k):
                        Topology.job_map.get(k)['state'] = v.get('state')
                        Topology.job_map.get(k)['last_time'] = v.get('last_time')
                        Topology.job_map.get(k)['last_state'] = v.get('last_state')
                        Topology.job_map.get(k)['manual_do'] = v.get('manual_do')
                    else:
                        logging.info('jobmapmsg 未发现任务jid {}'.format(k))

        def handle(self):
            try:
                whole_data = ''
                data = self.request.recv(Topology.buf_size)
                max_count = 0
                while True:
                    max_count += 1
                    if max_count > 100:
                        raise Exception('tcp data incomplete')
                    whole_data += to_str(data)
                    if to_str(data).endswith('\r\n\r\n'):
                        break
                    data = self.request.recv(Topology.buf_size)
                if 'HTTP' in whole_data:
                    self.handle_http(whole_data)
                else:
                    self.handle_msg(whole_data)
            except Exception as e:
                logging.exception('handle data error {} {}'.format(e, whole_data))
            finally:
                self.request.close()

    def server(self, srv):
        logging.info('pid: {},TCP服务线程启动'.format(get_pid()))
        srv.serve_forever()

    def run(self):
        threading.Thread(target=self.server, kwargs={'srv': self.srv}).start()
        logging.info('pid: {},网络拓扑维护线程启动'.format(get_pid()))
        while self.is_running:
            time.sleep(1)
            self.heartbeat()
            self.check()
        self.srv.shutdown()
        logging.info('网络拓扑维护线程退出')

    def stop(self):
        self.is_running = False

    def need_execute(self, job_id):

        if self.job_map.get(job_id).get('state') == 'off':
            if not self.job_map.get(job_id).get('manual_do'):
                return False

        m = hashlib.md5()
        m.update(job_id.encode('utf-8'))
        key = int(m.hexdigest(), 16)
        index = key % len(self.topology)
        self.topology.sort()
        if self.topology[index] == self.hostname:
            return True
        else:
            if self.topology[index] == '127.0.0.1' or self.topology[index] == 'localhost':
                return True
            return False

    @classmethod
    def get_host(cls, job_id):

        m = hashlib.md5()
        m.update(job_id.encode('utf-8'))
        key = int(m.hexdigest(), 16)
        index = key % len(cls.topology)
        cls.topology.sort()
        return cls.topology[index]

    @classmethod
    def check(cls):
        if len(cls.servers) == 1:
            return

        msg = {'check': cls.hostname}
        jmsg = json.dumps(msg) + '\r\n\r\n'
        index = cls.servers.index(cls.hostname)

        if index == len(cls.servers) - 1:
            sendhost = cls.servers[0]
        else:
            sendhost = cls.servers[index + 1]

        if sendhost in cls.topology:
            cls.check_count = 0
            return
        else:
            if cls.check_count < cls.check_dump_interval:
                cls.check_count += 1
                return
            else:
                cls.check_count = 0

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((sendhost, cls.port))
            sock.sendall(to_bytes(jmsg))

            try:
                whole_data = ''
                data = sock.recv(Topology.buf_size)
                while True:
                    whole_data += to_str(data)
                    if len(data) < Topology.buf_size or len(data) == Topology.buf_size and to_str(data).endswith(
                            '\r\n\r\n'):
                        break
                    data = sock.recv(Topology.buf_size)
            except Exception as e:
                logging.exception('check data error {}'.format(e))
            finally:
                sock.close()

            jresp = json.loads(whole_data)

            if jresp.get('check_resp'):
                for host in jresp.get('check_resp'):
                    if host not in cls.topology:
                        cls.topology.append(host)
                        logging.info('拓扑更新 {}'.format(Topology.topology))
                cls.broadcast()

        except Exception as e:
            logging.error('send check error:{}'.format(e))

    @classmethod
    def heartbeat(cls):

        if len(cls.topology) == 1:
            return

        msg = {'heartbeat': cls.hostname}
        jmsg = json.dumps(msg) + '\r\n\r\n'
        cls.topology.sort()
        index = cls.topology.index(cls.hostname)

        if index == len(cls.topology) - 1:
            sendhost = cls.topology[0]
        else:
            sendhost = cls.topology[index + 1]

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((sendhost, cls.port))
            sock.sendall(to_bytes(jmsg))
            sock.close()
        except Exception as e:
            logging.error('send heartbeat error:{}'.format(e))
            cls.topology.remove(sendhost)
            logging.info('拓扑更新 {}'.format(Topology.topology))
            cls.broadcast()

    @classmethod
    def broadcast(cls):

        if len(cls.topology) == 1:
            return

        hostlist = {}

        for host in cls.servers:
            if host in cls.topology:
                hostlist[host] = True
            else:
                hostlist[host] = False

        msg = {'broadcast': {}}

        msg.get('broadcast')['server_map'] = hostlist

        job_map = {}

        for job_id, job in Topology.job_map.items():
            job_map[job_id] = {}
            job_map[job_id]['state'] = job.get('state')
            job_map[job_id]['last_state'] = job.get('last_state')
            job_map[job_id]['last_time'] = job.get('last_time')
            job_map[job_id]['manual_do'] = job.get('manual_do')

        msg.get('broadcast')['job_map'] = job_map

        jmsg = json.dumps(msg) + '\r\n\r\n'

        hosts = list(cls.topology)
        hosts.remove(cls.hostname)

        try:
            for sendhost in hosts:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((sendhost, cls.port))
                sock.sendall(to_bytes(jmsg))
                sock.close()
        except Exception as e:
            logging.error('send topology_broadcast error:{}'.format(e))
            cls.topology.remove(sendhost)
            logging.info('拓扑更新 {}'.format(Topology.topology))
            cls.broadcast()

    @classmethod
    def jobmapmsg(cls):

        if len(cls.topology) == 1:
            return

        msg = {'jobmapmsg': {}}

        job_map = {}

        for job_id, job in Topology.job_map.items():
            job_map[job_id] = {}
            job_map[job_id]['state'] = job.get('state')
            job_map[job_id]['last_state'] = job.get('last_state')
            job_map[job_id]['last_time'] = job.get('last_time')
            job_map[job_id]['manual_do'] = job.get('manual_do')

        msg.get('jobmapmsg')['job_map'] = job_map

        jmsg = json.dumps(msg) + '\r\n\r\n'

        hosts = list(cls.topology)
        hosts.remove(cls.hostname)

        for sendhost in hosts:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((sendhost, cls.port))
                sock.sendall(to_bytes(jmsg))
                sock.close()
            except Exception as e:
                logging.error('send jobmapmsg error:{}'.format(e))
                continue

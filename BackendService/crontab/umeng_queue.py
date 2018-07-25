# -*- coding: utf-8 -*-

import os
import sys
import yaml
import time
import redis
import logging
import json

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..'))

from libs.umeng.push import Push

class UmengQueue:

    def __init__(self):
        umengconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/umeng.yaml'))
        self.and_push_conf = umengconf['push']['and']
        self.ios_push_conf = umengconf['push']['ios']
        self.and_push = Push(app_key=self.and_push_conf['app_key'], app_master_secret=self.and_push_conf[
            'app_master_secret'])
        self.ios_push = Push(app_key=self.ios_push_conf['app_key'], app_master_secret=self.ios_push_conf[
            'app_master_secret'])


        cacheconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/cache.yaml'))
        redisconf = cacheconf['redis']
        self.redis = redis.Redis(host=redisconf['host'],
                                 port=redisconf['port'],
                                 db=redisconf['dbindex'],
                                 password=redisconf['password'])
        self.umeng_queue_key = 'umeng-queue'

    def __call__(self):
        while 1:
            body = self.get_body()
            if body:
                self.deal(body=body)

            time.sleep(0.1)

    def deal(self, body):
        '''
        :param body: 'platform', 'device_token', 'title', 'subtitle', 'body', 'extra'
        :return:
        '''
        logging.info('send push: %s' % str(body))
        data = json.loads(body)
        if type(data) != dict or 'platform' not in data or 'device_token' not in data:
            return False

        if data.get('platform') == 'and':
            res = self.and_push.send_android_unicast(device_token=data.get('device_token'), ticker=data.get('title'),
                                           title=data.get('subtitle'), text=data.get('body'),
                                           extra=data.get('extra'), development=int(self.and_push_conf['development']))
            logging.info('send push result: %s %s' % (str(res.status_code), str(res.text)))
        elif data.get('platform') == 'ios':
            res = self.ios_push.send_ios_unicast(device_token=data.get('device_token'), title=data.get('title'),
                                       subtitle=data.get('subtitle'), body=data.get('body'),
                                       extra=data.get('extra'), development=int(self.ios_push_conf['development']))
            logging.info('send push result: %s %s' % (str(res.status_code), str(res.text)))
        return

    def get_body(self):
        body = self.redis.lpop(self.umeng_queue_key)
        if body:
            return body
        else:
            return ''

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    queue = UmengQueue()
    queue()
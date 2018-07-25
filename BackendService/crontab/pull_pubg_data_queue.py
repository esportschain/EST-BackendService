# -*- coding: utf-8 -*-

import os
import sys
import yaml
import time
import redis
import logging

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..'))

from worker.pull_pubg_official import *


class PullPubgDataQueue:

    def __init__(self):
        cacheconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/cache.yaml'))
        redisconf = cacheconf['redis']
        self.redis = redis.Redis(host=redisconf['host'],
                                 port=redisconf['port'],
                                 db=redisconf['dbindex'],
                                 password=redisconf['password'])
        self.pubg_queue_key_distinct = 'pull-data-distinct'
        self.pubg_queue_key = 'pull-data-queue'

    def __call__(self):
        while 1:
            nickname = self.get_account()
            if nickname:
                self.deal(nickname=str(nickname, 'utf-8'))

            time.sleep(0.1)

    def deal(self, nickname):
        print('pubg account: %s' % nickname)
        logging.info('pubg account: %s' % nickname)
        pubg_data_pull = PullPubgData()
        rst = pubg_data_pull(nickname=nickname)
        if rst:
            pubg_data_stats = PlayerStats()
            pubg_data_stats(nickname=nickname)
        return

    def get_account(self):
        nickname = self.redis.lpop(self.pubg_queue_key)
        if nickname:
            self.redis.srem(self.pubg_queue_key_distinct, nickname)
            return nickname
        else:
            return ''

if __name__ == '__main__':

    queue = PullPubgDataQueue()
    queue()
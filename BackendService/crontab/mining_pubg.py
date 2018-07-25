# -*- coding: utf-8 -*-

import sys
import os
import yaml
import logging
import random
import time

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..'))

from libs.mysql.mysql_db import MySQLBase
from libs.tools import *
from datetime import datetime, timezone

class MiningPubg:

    OPTYPE_MINING = 1   
    OPTYPE_EXTRACT = 2  

    TYPE_INCREASE = 1   
    TYPE_DECREASE = 2   

    def __init__(self):
        dbconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/db.yaml'))
        conf = dbconf['master_db']
        self.mdbcur = MySQLBase(host=conf['host'], user=conf['user'], password=conf['password'], database=conf[
            'database'])
        if not self.mdbcur.ready():
            logging.error('init db error, msg:%s' % self.mdbcur.get_error_msg())

        estconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/est.yaml'))
        pubg_mining_conf = estconf['mining_pubg']

        self.est_day_max = pubg_mining_conf['est_day_max']
        self.est_day_max_person = pubg_mining_conf['est_day_max_person']
        self.est_every_time = pubg_mining_conf['est_every_time']
        self.est_day_total = 0

        self.start_time = datetime.fromtimestamp(int(time.time()) - 86400).strftime("%Y-%m-%d %H:%M:%S")
        # self.start_time = 0
        self.predefine = random.randint(0, 999999)
        # self.predefine = 20555

    def __call__(self):
        cpage = 1
        while 1:
            account_list = self.get_account_list(cpage=cpage, prepage=1000)
            if not account_list:
                break

            for account in account_list:
                user_get_est = 0
                uid = account.get('uid')
                pd_list = self.get_player_data(nickname=account.get('accountname'), start_time=self.start_time)
                for pd in pd_list:
                    if self.check_person_day_max(current_get_est=user_get_est):
                        logging.info('%s exceed est person day max' % str(uid))
                        break

                    if self.check_award(pd=pd):
                        user_get_est += self.est_every_time
                        self.est_day_total += self.est_every_time

                if self.check_est_day_max():
                    break

                if user_get_est > 0:
                    self.add_est(uid=uid, est=user_get_est, pd_cnt=len(pd_list))
            cpage += 1

    def check_award(self, pd):
        return int(pd.get('calculation_force')) == self.predefine

    def check_person_day_max(self, current_get_est):
        return current_get_est + self.est_every_time > self.est_day_max_person

    def check_est_day_max(self):
        return self.est_day_total > self.est_day_max

    def add_est(self, uid, est, pd_cnt):
        print('uid: %s, est: %s, pdcnt: %s' % (uid, est, pd_cnt))
        est_data = {
            'uid': uid,
            'type': self.TYPE_INCREASE,
            'money': est,
            'obtype': self.OPTYPE_MINING,
            'obid': '%s_%s_%s' % (datetime.now().strftime('%Y%m%d%H%M%S'), '9', str(pd_cnt)),
            'status': 1,
            'created': int(time.time()),
            'updated': 0,
            'deleted': 0
        }
        where = 'uid = %s AND status != 5' % str(uid)
        try:
            res1 = self.mdbcur.transaction_insert(table='money_est', data=est_data)
            res2 = self.mdbcur.transaction_incr(table='member_user', field='money', where=where, unit=est)
            print(res1, res2)
            if res1 > 0 and res2 > 0:
                self.mdbcur.commit()
            else:
                logging.error(self.mdbcur.get_error_msg())
                self.mdbcur.rollback()
        except Exception as e:
            logging.error('error: %s, Received %r' % (str(e), est_data))
            self.mdbcur.rollback()

    def get_account_list(self, cpage, prepage):
        where = "status = 1"
        return self.mdbcur.fetch_more(table='game_user_account', where=where, fields='uid,accountname',
                                      limit=(cpage-1)*prepage, size=prepage, order='id ASC')

    def get_player_data(self, nickname, start_time):
        where = "nickname='%s' AND start_time>='%s'" % (nickname, start_time)
        pd_list = self.mdbcur.fetch_more(table=get_pubg_pd_table(nickname=nickname), where=where,
                                         order='start_time desc', fields='calculation_force')
        if type(pd_list) != list:
            return False

        return pd_list

if __name__ == '__main__':
    # print(type(-110) is int)
    # print(type(True) is int)
    # print(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    # print(type(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    m = MiningPubg()
    m()
    # match_creation = '2018-06-22T00:00:00Z'
    # a = int(time.mktime(datetime.strptime(match_creation, '%Y-%m-%dT%H:%M:%SZ').timetuple())) + 28800
    # times = datetime.fromtimestamp(a)
    # print(times.strftime("%Y-%m-%d %H:%M:%S"))
    # print(datetime.fromtimestamp(int(time.time()) - 28800).strftime("%Y-%m-%d %H:%M:%S"))
    # match_creation = datetime.strptime(match_creation, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
    #
    # start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # print(time.strftime('%Y-%m-%d %H:%M:%S'))
    # print(type(time.strftime('%Y-%m-%d %H:%M:%S')))
    # print(start_time)
    # print(type(start_time))
# -*- coding: utf-8 -*-

import logging
import requests
import json
import yaml
import os
import time
import sys

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..'))
print(sys.path)

from libs.mysql.mysql_db import MySQLBase
from decimal import *
from web3 import Web3
from web3.utils.threads import Timeout

from libs.transaction_maker import TransactionMaker
from libs.umeng.push import Push
from libs.smtp_email import SmtpEmail

class SendMoney:

    def __init__(self):
        dbconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/db.yaml'))
        conf = dbconf['master_db']
        self.mdbcur = MySQLBase(host=conf['host'], user=conf['user'], password=conf['password'], database=conf[
            'database'])
        if not self.mdbcur.ready():
            logging.error('init db error, msg:%s' % self.mdbcur.get_error_msg())

        umengconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/umeng.yaml'))
        self.and_push_conf = umengconf['push']['and']
        self.ios_push_conf = umengconf['push']['ios']
        self.and_push = Push(app_key=self.and_push_conf['app_key'], app_master_secret=self.and_push_conf[
            'app_master_secret'])
        self.ios_push = Push(app_key=self.ios_push_conf['app_key'], app_master_secret=self.ios_push_conf[
            'app_master_secret'])

        estconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/est.yaml'))
        base_conf = estconf['base']

        self.provider_url = base_conf['provider_url']
        self.contract_address = base_conf['contract_address']
        self.erc20_abi = base_conf['erc20_abi']
        self.from_address = base_conf['from_address']
        self.chain_id = base_conf['chain_id']
        self.alert_min_eth = int(base_conf['alert_min_eth'])

        self.trans = TransactionMaker(provider_url=self.provider_url, my_wallet_address=self.from_address,
                                      conf=base_conf['private_key'])
        self.trans.set_abi_type(erc20_abi_json=self.erc20_abi)

        self.email = None

    def __call__(self):

        if self.trans.check_from_address_eth(eth=self.alert_min_eth):

            self.send_email(title='accout alert', content='not enough' % str(self.trans.get_eth_balance(
                address=self.from_address)))
            return

        cpage = 1
        while 1:
            account_list = self.get_apply_account_list(cpage=cpage, prepage=1000)
            print(account_list)
            if not account_list:
                break

            for a in account_list:
                to_address = a.get('address')
                money = self.est_to_wei(v=a.get('money'))
                tx_hex, gas_price_gwei, nonce, cost_est = self.send_est(to=to_address, value=money)

                if tx_hex:
                    res = self.wait_receipt(tx_hex=tx_hex)
                    if res:
                        log = {
                            'apply_id': a.get('id'),
                            'txhash': tx_hex,
                            'nonce': nonce,
                            'total_cost': Web3.fromWei((gas_price_gwei * int(res.gasUsed) * 10 ** 9), 'ether'),
                            'result': res.status,
                            'log': str(res)
                        }
                        u_apply = {
                            'txhash': tx_hex,
                            'status': 3 if int(res.status) == 1 else 4,
                            'updated': int(time.time())
                        }
                        self.add_log(data=log)
                        self.update_apply(id=a.get('id'), data=u_apply)
                        if int(res.status) == 1:
                            self.add_push(uid=a.get('uid'), apply_money=a.get('money'), procedures_money=cost_est)
                    else:
                        continue

            cpage += 1

        if self.email:
            self.email.quit()

    def add_log(self, data):
        return self.mdbcur.insert(table='hash_log', data=data)

    def update_apply(self, id, data):
        where = 'id = %s' % id
        return self.mdbcur.update(table='extra_apply', data=data, where=where)

    def est_to_wei(self, v):
        return int(float(v) * 10 ** 9)

    def get_apply_account_list(self, cpage, prepage):
        where = "status = 1 AND txhash = ''"
        return self.mdbcur.fetch_more(table='extra_apply', where=where,
                                       limit=(cpage-1)*prepage, size=prepage, order='id ASC')

    def send_est(self, to, value, gas_extra=1):
        try:
            return self.trans.send_erc20(to=to, value=value,
                                           contract_address=self.contract_address, chain_id=self.chain_id,
                                           gas_extra=gas_extra)
        except ValueError as e:
            if SendMoney.is_json(e):
                a = str(e)
                a = a.replace('\'', '"')
                rst = json.loads(a)
                if int(rst.get('code')) == -32000 and rst.get('message') == 'intrinsic gas too low':
                    return self.send_est(to=to, value=value, gas_extra=gas_extra * 1.2)
        return '',  0, 0, 0

    def wait_receipt(self, tx_hex):
        try:
            return self.trans.fetch_transaction_receipt(tx_hex=tx_hex)
        except Timeout as e:
            return self.wait_receipt(tx_hex=tx_hex)

    def find_user(self, uid):
        where = 'uid = %s AND status != 5' % uid
        return self.mdbcur.fetch_one(table='member_user', where=where)

    def add_push(self, uid, apply_money, procedures_money):
        user = self.find_user(uid=uid)
        real_money = float(apply_money) - float(procedures_money)
        if type(user) != dict or not user.get('platform') or not user.get('device_token'):
            return False

        extra = {
            'type': 1,
            'msg': 'Your EST withdrawals have been successful. '
                   'The cash amount is %s, '
                   'the handling fee is %s, '
                   'and the actual account is %s. '
                   'Please check the receipt.' % (str(apply_money), str(procedures_money), str(real_money))
        }
        if user.get('platform') == 'and':
            self.and_push.send_android_unicast(device_token=user.get('device_token'), ticker='Notice of presentation',
                                           title='Notice of presentation', text='Est cash withdrawal success',
                                           extra=extra, development=int(self.and_push_conf['development']))
        elif user.get('platform') == 'ios':
            self.ios_push.send_ios_unicast(device_token=user.get('device_token'), title='Notice of presentation',
                                       subtitle='Notice of presentation', body='Est cash withdrawal success',
                                       extra=extra, development=int(self.ios_push_conf['development']))
        else:
            return False

    def send_email(self, title, content):
        if not self.email:
            self.emailconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/email.yaml'))
            self.email = SmtpEmail(host=self.emailconf['host'], port=self.emailconf['port'], user=self.emailconf['user'],
                                   pwd=self.emailconf['pwd'], smtp_crypto=self.emailconf['crypto'])

        self.email.send(from_mail=self.emailconf['user'], to_mail=self.emailconf['notice_list'], title=title,
                        content=content)

    @staticmethod
    def is_json(raw_msg):
        if isinstance(raw_msg, str):
            try:
                json.loads(raw_msg)
            except ValueError:
                return False
            return True
        else:
            return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    s = SendMoney()
    s()


# -*- coding: utf-8 -*-
# !/usr/bin/env python

import logging
import yaml
import os

from tornado.escape import json_encode
from tornado.ioloop import IOLoop
from tornado.options import define, options, parse_command_line
from tornado.web import Application, RequestHandler

from web3 import Web3
from libs.transaction_maker import TransactionMaker

define('port', default=11211, help="port to listen on")
define('host', default='127.0.0.1', help="host to listen on")
define('debug', default=False, group='application',
       help="run in debug mode (with automatic reloading)")

estconf = yaml.load(open(os.path.dirname(__file__) + '/conf/est.yaml'))

class BaseHandler(RequestHandler):

    def get(self):
        self.write_error(404)

    def write_error(self, status_code, **kwargs):
        data = dict()
        data["ret"] = data["code"] = status_code
        data["msg"] = "error"
        data['data'] = dict()
        self.write(json_encode(data))


class MainHandler(BaseHandler):
    """
    """

    def get(self):
        pass


class EstAccountHandler(BaseHandler):


    def get(self):
        ret = {'ret': 0, 'code': 0, 'msg': '', 'data': dict()}
        address = self.get_argument('address', '', True)
        est = self.get_argument('est', '', True)
        if not address or not est:
            ret['ret'] = ret['code'] = 400
            ret['msg'] = 'address or est cannot be empty'
            self.write(json_encode(ret))
            return

        address = Web3.toChecksumAddress(address)
        if not Web3.isChecksumAddress(address):
            ret['ret'] = ret['code'] = 400
            ret['msg'] = 'address invalid'
            self.write(json_encode(ret))
            return

        base_conf = estconf['base']

        trans = TransactionMaker(provider_url=base_conf['provider_url'], my_wallet_address=base_conf['from_address'])
        trans.set_abi_type(erc20_abi_json=base_conf['erc20_abi'])

        value = int(float(est) * 10 ** 9)
        cost = trans.get_transfer_cost(to=address, value=value, contract_address=base_conf['contract_address'])
        if not cost:
            ret['ret'] = ret['code'] = 400
            ret['msg'] = 'System maintenance'
            self.write(json_encode(ret))
            return
        ret['data'] = {'cost': cost}

        self.write(json_encode(ret))
        return



def main():
    parse_command_line()

    app = Application([(r"/", MainHandler), (r"/est", EstAccountHandler), (r".*", BaseHandler), ])
    print(options.port)
    app.listen(options.port, address='127.0.0.1')
    logging.info('Listening on http://localhost:%d' % options.port)

    IOLoop.current().start()


if __name__ == '__main__':
    main()

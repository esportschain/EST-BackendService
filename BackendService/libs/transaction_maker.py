# -*- coding: utf-8 -*-

import json
import logging
import requests
import base64

from web3 import HTTPProvider, Web3

class TransactionMaker:

    
    def __init__(self, provider_url, my_wallet_address, conf=None):
        self.web3 = Web3(HTTPProvider(provider_url))
        self.my_wallet_address = my_wallet_address
        self.exchange_rate = None
        self.private_key = None
        self.conf = conf

    def set_abi_type(self, erc20_abi_json):
        self.erc20_abi = json.loads(erc20_abi_json)

    def sign_transaction(self, nonce, gas_price, gas, to, value, data_hex_str='', chain_id=1):
        
        private_key = self.get_private_key()

        to = Web3.toChecksumAddress(to)
        transaction = {
            'nonce': nonce,
            'gasPrice': gas_price,
            'gas': gas,
            'to': to,
            'value': value,
            'data': data_hex_str,
            'chainId': chain_id
        }

        logging.info("will sign transaction: {}".format(transaction))
        signed_tx = self.web3.eth.account.signTransaction(transaction, private_key)
        return signed_tx.rawTransaction

   
    def send_signed_transaction(self, raw_txn):
        logging.info("--- sending raw hex: {}".format(raw_txn.hex()))
        txid = self.web3.eth.sendRawTransaction(raw_txn)
        txHex = txid.hex()
        logging.info("--- sendding transaction hash: {}".format(txHex))
        return txHex

    def onchain_all_transactions(self):
        return self.web3.eth.getTransactionCount(self.my_wallet_address, "pending")

    def next_nonce(self):
       
        local_last_nonce = 0
        last_nonce = local_last_nonce

        if last_nonce == 0:
            last_nonce = self.onchain_all_transactions()
            logging.info("Onchain last nonce {}".format(last_nonce))

        return last_nonce

    def get_recommand_gas_price(self):
       
        uri = "https://www.etherchain.org/api/gasPriceOracle"
        r = requests.get(uri)
        logging.info(r)
        if r.status_code == requests.codes.ok:
            return float(r.json()['fast'])
        else:
            return 1.1

    def send_eth(self, to, value, gas_price_gwei=None, gas=21000, chain_id=1):
        nonce = self.next_nonce()

        if gas_price_gwei is None:
            gas_price_gwei = self.get_recommand_gas_price()
            pass

        gas_price = int(gas_price_gwei * 10 ** 9)
        raw_txn = self.sign_transaction(nonce, gas_price, gas, to, value, chain_id=chain_id)
        return self.send_signed_transaction(raw_txn)

    def send_erc20(self, to, value, contract_address, gas_price_gwei=None, gas=240000, chain_id=1, gas_extra=1):
        nonce = self.next_nonce()

        to = Web3.toChecksumAddress(to)
        contract_address = Web3.toChecksumAddress(contract_address)

        if gas_price_gwei is None:
            gas_price_gwei = self.get_recommand_gas_price()
            pass

       
        target_contract = self.web3.eth.contract(abi=self.erc20_abi, address=contract_address)
        txn = target_contract.functions.transfer(to, value).buildTransaction({'gas': gas})

        estimate_gas = self.web3.eth.estimateGas({"to": txn["to"], "from": self.my_wallet_address, "data": txn["data"], "value": txn["value"]})
        print("estimated gas %d" % estimate_gas)
        
        estimate_gas *= 2
        estimate_gas = int(estimate_gas * gas_extra)

      
        total_cast_eth = Web3.fromWei((gas_price_gwei * estimate_gas * 10 ** 9), 'ether')
        total_cast_est = self.get_eth_to_est(eth_coin=total_cast_eth)
        if not total_cast_est:
            return -3, gas_price_gwei, nonce, 0

        value = value - int(int(total_cast_est) * 10 ** 9)
        if value <= 0:
            logging.info('cost eth overtop transfer accounts est')
            return -1, gas_price_gwei, nonce, int(total_cast_est)

        if self.check_from_address_est(contract_address=contract_address, value=value):
            logging.info('from_address est_balance not enough')
            return -2, gas_price_gwei, nonce, int(total_cast_est)

        txn = target_contract.functions.transfer(to, value).buildTransaction({'gas': gas})
        data = txn["data"]

        gas_price = int(gas_price_gwei * 10 ** 9)
        raw_txn = self.sign_transaction(nonce=nonce, gas_price=gas_price, gas=estimate_gas, to=contract_address, value=0,
                                        data_hex_str=data, chain_id=chain_id)
        return self.send_signed_transaction(raw_txn), gas_price_gwei, nonce, int(total_cast_est)

    def send_est(self, to, value, chain_id=1):
        return self.send_erc20(to, value, '0x*********************c0', chain_id=chain_id)

    def fetch_transaction_receipt(self, tx_hex):
        return self.web3.eth.waitForTransactionReceipt(tx_hex)

    def get_transaction_receipt(self, tx_hex):
        return self.web3.eth.getTransactionReceipt(tx_hex)

    def fetch_latest_block_number(self):
        block = self.web3.eth.getBlock('latest')
        return block['number']

    def get_erc20_balance(self, address, contract_address):
        contract_address = Web3.toChecksumAddress(contract_address)
        target_contract = self.web3.eth.contract(abi=self.erc20_abi, address=contract_address)
        return target_contract.call().balanceOf(address)

    def get_eth_balance(self, address):
        return self.web3.eth.getBalance(address)

    def check_from_address_eth(self, eth):
        eth_balance = self.get_eth_balance(address=self.my_wallet_address)
        result = int(eth_balance) < int(Web3.toWei(eth, 'ether'))
        return result

    def check_from_address_est(self, contract_address, value):
        est_balance = self.get_erc20_balance(address=self.my_wallet_address, contract_address=contract_address)
        result = int(est_balance) < int(value)
        return result

    def get_eth_to_est(self, eth_coin):
        if not self.exchange_rate:
            uri = ""
            r = requests.get(uri)
            logging.info(r)
            if r.status_code == requests.codes.ok:
                try:
                    ret = r.json()
                    eth_btc_sell = float(ret.get('data').get('eth_btc').get('sell'))
                    est_btc_sell = float(ret.get('data').get('est_btc').get('sell'))
                    self.exchange_rate = 1.0 * eth_btc_sell / est_btc_sell
                except Exception:
                    return False
            else:
                return False

        return float(eth_coin) * self.exchange_rate

    def get_transfer_cost(self, to, value, contract_address, gas_price_gwei=None, gas=240000, gas_extra=1):
        to = Web3.toChecksumAddress(to)
        contract_address = Web3.toChecksumAddress(contract_address)

        if gas_price_gwei is None:
            gas_price_gwei = self.get_recommand_gas_price()
            pass


        target_contract = self.web3.eth.contract(abi=self.erc20_abi, address=contract_address)
        txn = target_contract.functions.transfer(to, value).buildTransaction({'gas': gas})

        estimate_gas = self.web3.eth.estimateGas(
            {"to": txn["to"], "from": self.my_wallet_address, "data": txn["data"], "value": txn["value"]})
        print("estimated gas %d" % estimate_gas)

        estimate_gas *= 2
        estimate_gas = int(estimate_gas * gas_extra)

        total_cast_eth = Web3.fromWei((gas_price_gwei * estimate_gas * 10 ** 9), 'ether')
        total_cast_est = self.get_eth_to_est(eth_coin=total_cast_eth)
        return total_cast_est

    def get_private_key(self):
        if not self.private_key:
            r = requests.post(self.conf['url'], headers={'Content-Type': 'application/json'}, data=json.dumps({'tk': self.conf['tk'], 'sig': self.conf['sig']}))
            logging.info(r)
            if r.status_code == requests.codes.ok and int(r.json()['Code']) == 0:
                tmp = r.json()['Data']
                tmp = str(base64.b64decode(tmp), encoding='utf-8')
                self.private_key = tmp
            else:
                self.private_key = ''

        return self.private_key
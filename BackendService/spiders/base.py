# coding=utf-8
from libs.download import Downloader

class SpidersBase:
    BASE_URL = 'https://api.playbattlegrounds.com/shards'

    def __init__(self, headers):
        self.d = Downloader(headers=headers, delay=0)

    def filter(self, params):
        if type(params) != dict:
            return ''
        return '&'.join(['filter[%s]=%s' % (k, v) for k, v in params.items()])


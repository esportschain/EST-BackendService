# coding=utf-8
import json
from spiders.base import SpidersBase

class Match(SpidersBase):

    def __call__(self, headers):
        SpidersBase.__init__(self, headers)

    def match(self, region, matchid):
        url = '%s/%s/matches/%s' % (self.BASE_URL, region, matchid)
        res = self.d(url)
        return json.loads(res) if res else ''

class Player(SpidersBase):

    def __call__(self, headers):
        SpidersBase.__init__(self, headers)

    def players(self, region, params):
        url = '%s/%s/players?%s' % (self.BASE_URL, region, self.filter(params=params))
        result = self.d(url=url, is_rsp_code=True)
        html = result.get('html') if 'html' in result else ''
        return json.loads(html) if html else '', result.get('code')

    def player(self, region, accountid):
        url = '%s/%s/players/%s' % (self.BASE_URL, region, accountid)
        res = self.d(url)
        return json.loads(res) if res else ''

    def player_season(self, region, accountid, season):
        url = '%s/%s/players/%s/seasons/%s' % (self.BASE_URL, region, accountid, season)
        res = self.d(url)
        return json.loads(res) if res else ''

    def seasons(self, region):
        url = '%s/%s/seasons' % (self.BASE_URL, region)
        res = self.d(url)
        return json.loads(res) if res else ''

if __name__ == '__main__':
    headers = {
        'Authorization': '',
        'accept': 'application/vnd.api+json'
    }
    region = 'pc-as'
    


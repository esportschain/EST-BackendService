# -*- coding: utf-8 -*-

import sys
import logging
import json
import yaml
import os
import time
import threading

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)), '..'))

from binascii import crc32
from spiders.client import *
from libs.mysql.mysql_db import MySQLBase
from datetime import datetime
from libs.tools import *

SLEEP_TIME = 0.1


def common_threaded_deal(deal_queue, region, accountid, existed_matchids, callback, max_threads=5):
    """ Crawl data of lol super server in multiple threads """
    def process_queue():
        while True:
            try:
                item = deal_queue.pop()
            except IndexError:
                # crawl queue is empty
                break
            else:
                callback.crawle_match(region=region, matchid=item.get('id'),
                                      accountid=accountid, existed_matchids=existed_matchids)
                logging.info('pulled pubg match of %s' % (item.get('id')))

    # wait for all download threads to finish
    threads = []
    while threads or deal_queue:
        # the crawl is still active
        for thread in threads:
            if not thread.is_alive():
                # remove the stopped threads
                threads.remove(thread)
        while len(threads) < max_threads and deal_queue:
            # can start some more threads
            thread = threading.Thread(target=process_queue)
            thread.setDaemon(True)  # set daemon so main thread can exit when receives ctrl-c
            thread.start()
            threads.append(thread)

        # all threads have been processed
        # sleep temporarily so CPU can focus execution on other threads
        time.sleep(SLEEP_TIME)

def callback_pull_pubg_official(ch, method, properties, body):

    logging.debug(" [x] Received %r" % body)
    logging.debug(" [x] Received %r" % method)
    logging.debug(" [x] Received %r" % properties)

    try:
        data = json.loads(body)
        nickname = data.get('nickname')
        p = PullPubgData()
        rst = p(nickname=nickname)
        if rst:
            ps = PlayerStats()
            ps(nickname=nickname)
    except Exception as e:
        logging.error('error: %s, Received %r' % (str(e), body))
        publish_message(ch=ch, message=body)

    if __name__ != '__main__':
        ch.basic_ack(delivery_tag=method.delivery_tag)

def publish_message(ch, message, exchange_name='pull_pubg_official'):
    logging.info('publish_message exchange_name %r' % message)
    if __name__ != '__main__':
        ch.basic_publish(exchange='', routing_key=exchange_name, body=message)

class PullPubgData:
    PLATFORM_REGION = [
        'pc-krjp',
        'pc-jp',
        'pc-na',
        'pc-eu',
        'pc-ru',
        'pc-oc',
        'pc-kakao',
        'pc-sea',
        'pc-sa',
        'pc-as'
    ]

    def __init__(self):
        dbconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/db.yaml'))
        self.conf = dbconf['master_db']
        self.mdbcur = MySQLBase(host=self.conf['host'], user=self.conf['user'], password=self.conf['password'],
                                database=self.conf['database'])
        if not self.mdbcur.ready():
            logging.error('init db error, msg:%s' % self.mdbcur.get_error_msg())

        self.header_key = 0
        self.init_pubg_client()

    def init_db(self):
        return MySQLBase(host=self.conf['host'], user=self.conf['user'], password=self.conf['password'], database=self.conf['database'])

    def init_pubg_client(self):
        self.headers = ['', '']

        header = {
            'Authorization': 'Bearer %s' % self.headers[self.header_key],
            'accept': 'application/vnd.api+json'
        }
        self.pubg_p = Player(headers=header)
        self.pubg_m = Match(headers=header)
        self.header_key += 1
        if self.header_key >= len(self.headers):
            self.header_key = 0

    def __call__(self, nickname):
        for region in self.PLATFORM_REGION:
           
            player, code = self.get_players(region=region, params={'playerNames': nickname})
            if type(player) != dict or 'data' not in player:
                continue

            if player.get('data'):
                accountid = player.get('data')[0].get('id')
                matchids = player.get('data')[0].get('relationships').get('matches').get('data')
                if matchids:
                    start_time = datetime.fromtimestamp(int(time.time()) - (86400 * 15)).strftime("%Y-%m-%d %H:%M:%S")
                    existed_matchids = self.get_recent_player_match(server=self.get_server(region=region),
                                                                    nickname=nickname, start_time=start_time)
                    common_threaded_deal(deal_queue=matchids, region=region, accountid=accountid,
                                         existed_matchids=existed_matchids, callback=self)

        return True

    def get_players(self, region, params):
        player, code = self.pubg_p.players(region=region, params=params)
        if code and int(code) == 429:
            self.init_pubg_client()
            return self.get_players(region=region, params=params)
        else:
            return player, code

    def crawle_match(self, region, matchid, accountid, existed_matchids):
        if matchid in existed_matchids:
            return

        match = self.pubg_m.match(region=region, matchid=matchid)
        print(matchid)
        if type(match) != dict or match.get('data').get('id') != matchid:
            return

        dbcur = self.init_db()
        match_data, player_data = self.handle_match(match=match, accountid=accountid)
        self.deal_match(dbcur=dbcur, match=match_data)
        self.deal_player(dbcur=dbcur, player_data=player_data)

    def get_recent_player_match(self, server, nickname, start_time):
        where = "server='%s' AND nickname='%s' AND start_time>='%s'" % (server, nickname, start_time)
        res = self.mdbcur.fetch_more(table=get_pubg_pd_table(nickname=nickname), where=where, fields='match_id')
        return [] if not res or type(res) is int else [x.get('match_id') for x in res]

    def get_last_time(self, server, nickname):
   
        where = "server = '%s' AND nickname = '%s'" % (server, nickname)
        res = self.mdbcur.fetch_one(table=get_pubg_pd_table(nickname=nickname), where=where,
                                    order='start_time DESC', fields='start_time')
        if type(res) != dict:
            return ''
        return res.get('start_time').strftime("%Y-%m-%d %H:%M:%S")

    def handle_match(self, match, accountid):
  
        players = {}
        player_data = {}
        queue_size_map = {'solo': 1, 'duo': 2, 'squad': 4, 'solo-fpp': 1, 'duo-fpp': 2, 'squad-fpp': 4, 'warmodetpp':
            5, 'warmodefpp': 5}
        # mode_map = {'solo': 'tpp', 'duo': 'tpp', 'squad': 'tpp', 'solo-fpp': 'fpp', 'duo-fpp': 'fpp', 'squad-fpp':
        #     'fpp', 'warmodetpp': 'tpp', 'warmodefpp': 'fpp'}
        server = self.get_server(match.get('data').get('attributes').get('shardId'))
        season = ''
        gameMode = match.get('data').get('attributes').get('gameMode')
        queue_size = queue_size_map.get(gameMode) if gameMode in queue_size_map else 99
        mode = 'fpp' if 'fpp' in gameMode else 'tpp'
        matchid = match.get('data').get('id')
        match_creation = match.get('data').get('attributes').get('createdAt')
        match_creation = datetime.strptime(match_creation, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d %H:%M:%S")
        total_rank = len(match.get('data').get('relationships').get('rosters').get('data'))
        match_data = {
            'match_id': matchid,
            'server': server,
            'season': season,
            'mode': mode,
            'queue_size': queue_size,
            'map_id': match.get('data').get('attributes').get('mapName'),
            'match_creation': match_creation,
            'base_stats_json': json.dumps(match)
        }
        for d in match.get('included'):
            if d.get('type') == 'roster':
                for a in d.get('relationships').get('participants').get('data'):
                    players[a.get('id')] = {
                        'team_rank': d.get('attributes').get('stats').get('rank'),
                        'team_id': d.get('attributes').get('stats').get('teamId'),
                        'total_rank': total_rank,
                    }

        for d in match.get('included'):
            if d.get('type') == 'participant' and d.get('id') in players:
                stats = d.get('attributes').get('stats')
                if stats.get('playerId') == accountid:
                    player_data = {
                        'account_id': stats.get('playerId'),
                        'season': season,
                        'server': server,
                        'queue_size': queue_size,
                        'mode': mode,
                        'match_id': matchid,
                        'start_time': match_creation,
                        'nickname': stats.get('name'),
                        'rank': stats.get('winPlace'),
                        'rating_delta': stats.get('winPointsDelta'),
                        'time_survived': stats.get('timeSurvived'),
                        'vehicle_destroys': stats.get('vehicleDestroys'),
                        'win_place': stats.get('winPlace'),
                        'kill_place': stats.get('killPlace'),
                        'heals': stats.get('heals'),
                        'weapon_acquired': stats.get('weaponsAcquired'),
                        'boosts': stats.get('boosts'),
                        'death_type': stats.get('deathType'),
                        'most_damage': stats.get('mostDamage'),
                        'kills': stats.get('kills'),
                        'assists': stats.get('assists'),
                        'kill_streaks': stats.get('killStreaks'),
                        'road_kills': stats.get('roadKills'),
                        'team_kills': stats.get('teamKills'),
                        'headshot_kills': stats.get('headshotKills'),
                        'longest_kill': stats.get('longestKill'),
                        'walk_distance': stats.get('walkDistance'),
                        'ride_distance': stats.get('rideDistance'),
                        'damage_dealt': stats.get('damageDealt'),
                        'knock_downs': stats.get('DBNOs'),
                        'revives': stats.get('revives'),
                        'team_rank': players.get(d.get('id')).get('team_rank'),
                        'team_id': players.get(d.get('id')).get('team_id'),
                        'total_rank': players.get(d.get('id')).get('total_rank'),
                        'participant_id': d.get('id'),
                        'calculation_force': self.get_calculation_force(stats=stats)
                    }

        return match_data, player_data

    def deal_match(self, dbcur, match):

        if 'deaths_json' not in match:
            match['deaths_json'] = ''
        where = "match_id = '%s' AND server = '%s'" % (match.get('match_id'), match.get('server'))
        res = dbcur.fetch_one(table='pubg_match_detail', where=where)
        if res:
            return 0
        return dbcur.insert(table='pubg_match_detail', data=match)

    def deal_player(self, dbcur, player_data):
    
        if 'honors' not in player_data:
            player_data['honors'] = ''
        dbcur.insert(table=get_pubg_pd_table(nickname=player_data.get('nickname')), data=player_data)

    def get_server(self, region):
        return region.split('-')[1]

    def get_calculation_force(self, stats):
        win_points = stats.get('winPoints')
        kill_points = stats.get('killPoints')
        return int(((0.9*win_points + 0.24*kill_points) - 1000) / 100) * stats.get('timeSurvived')


class PlayerStats:

    def __init__(self):
        dbconf = yaml.load(open(os.path.dirname(__file__) + '/../conf/db.yaml'))
        conf = dbconf['master_db']
        self.mdbcur = MySQLBase(host=conf['host'], user=conf['user'], password=conf['password'], database=conf[
            'database'])
        if not self.mdbcur.ready():
            logging.error('init db error, msg:%s' % self.mdbcur.get_error_msg())

    def __call__(self, nickname):
        pd_list = self.get_player_data(nickname=nickname)
        self.handle_bind(nickname=nickname, pd_list=pd_list)
        if not pd_list:
            logging.info('no player data')
            return True

        pd_sum_list = self.group_and_sum(pd_list=pd_list)

        for key, pd_sum in pd_sum_list.items():
            pd_sum['account_id'] = pd_list[0].get('account_id')
            pd_sum['account_name'] = nickname
            pd_sum['kdr'] = pd_sum.get('kills') / max(1, pd_sum.get('deaths'))
            pd_sum['win_rate'] = pd_sum.get('win_num') / pd_sum.get('match_num')
            pd_sum['top_10_rate'] = pd_sum.get('top_10_num') / pd_sum.get('match_num')
            pd_sum['win_top_10_rate'] = pd_sum.get('top_10_num') / pd_sum.get('match_num')
            pd_sum['damage_per_match'] = pd_sum.get('damage_dealt') / pd_sum.get('match_num')
            pd_sum['headshot_kills_per_match'] = pd_sum.get('headshot_kills') / pd_sum.get('match_num')
            pd_sum['heals_per_match'] = pd_sum.get('heals') / pd_sum.get('match_num')
            pd_sum['kills_per_match'] = pd_sum.get('kills') / pd_sum.get('match_num')
            pd_sum['move_distance_per_match'] = pd_sum.get('move_distance') / pd_sum.get('match_num')
            pd_sum['revives_per_match'] = pd_sum.get('revives') / pd_sum.get('match_num')
            pd_sum['road_kills_per_match'] = pd_sum.get('road_kills') / pd_sum.get('match_num')
            pd_sum['team_kills_per_match'] = pd_sum.get('team_kills') / pd_sum.get('match_num')
            pd_sum['time_survived_per_match'] = pd_sum.get('time_survived') / pd_sum.get('match_num')
            pd_sum['top_10_per_match'] = pd_sum.get('top_10_num') / pd_sum.get('match_num')
            pd_sum['loss_num'] = pd_sum.get('match_num') - pd_sum.get('win_num')
            pd_sum['headshot_kill_rate'] = pd_sum.get('headshot_kills') / max(1, pd_sum.get('kills'))
            pd_sum['avg_survival_time'] = pd_sum.get('time_survived') / pd_sum.get('match_num')
            pd_sum['avg_walk_distance'] = pd_sum.get('walk_distance') / pd_sum.get('match_num')
            pd_sum['avg_ride_distance'] = pd_sum.get('ride_distance') / pd_sum.get('match_num')
            pd_sum['avg_rank'] = pd_sum.get('avg_rank') / pd_sum.get('match_num')
            pd_sum['kill_place_per_match'] = pd_sum.get('kill_place') / pd_sum.get('match_num')
            pd_sum['boosts_per_match'] = pd_sum.get('boosts') / pd_sum.get('match_num')
            pd_sum['assists_per_match'] = pd_sum.get('assists') / pd_sum.get('match_num')
            pd_sum['knock_downs_per_match'] = pd_sum.get('knock_downs') / pd_sum.get('match_num')
            pd_sum['weapon_acquired_per_match'] = pd_sum.get('weapon_acquired') / pd_sum.get('match_num')

            pd_sum['tmp']['headshot_kills_rate'] = pd_sum['tmp']['headshot_kills_rate'] / max(1, pd_sum['tmp']['has_kills_match_num'])
            pd_sum['tmp']['solo_kills_per_match'] = pd_sum['tmp']['solo_kills'] / max(1, pd_sum['tmp']['solo_match_num'])
            pd_sum['tmp']['team_kills_per_match'] = pd_sum['tmp']['team_kills'] / max(1, pd_sum['tmp']['team_match_num'])
            pd_sum['tmp']['solo_heals_per_match'] = pd_sum['tmp']['solo_heals'] / max(1, pd_sum['tmp']['solo_match_num'])
            pd_sum['tmp']['team_heals_per_match'] = pd_sum['tmp']['team_heals'] / max(1, pd_sum['tmp']['team_match_num'])
            pd_sum['tmp']['solo_ride_distance_per_match'] = pd_sum['tmp']['solo_ride_distance'] / max(1, pd_sum['tmp']['solo_match_num'])
            pd_sum['tmp']['team_ride_distance_per_match'] = pd_sum['tmp']['team_ride_distance'] / max(1, pd_sum['tmp']['team_match_num'])
            pd_sum['tmp']['solo_weapon_acquired_per_match'] = pd_sum['tmp']['solo_weapon_acquired'] / max(1, pd_sum['tmp']['solo_match_num'])
            pd_sum['tmp']['team_weapon_acquired_per_match'] = pd_sum['tmp']['team_weapon_acquired'] / max(1, pd_sum['tmp']['team_match_num'])
            pd_sum['tmp']['kill_streaks_per_match'] = pd_sum['tmp']['kill_streaks'] / pd_sum['match_num']

            del pd_sum['tmp']
            self.save_player_stats(pd_sum)

    def get_player_data(self, nickname):
        where = "nickname='%s'" % nickname
        pd_list = self.mdbcur.fetch_more(table=get_pubg_pd_table(nickname=nickname), where=where, order='start_time desc')
        if type(pd_list) != list:
            return False

        return pd_list

    @property
    def default_stats(self):
        return {
            'avg_rank': 0,
            'time_survived': 0,
            'vehicle_destroys': 0,
            'heals': 0,
            'weapon_acquired': 0,
            'boosts': 0,
            'kills': 0,
            'assists': 0,
            'road_kills': 0,
            'team_kills': 0,
            'headshot_kills': 0,
            'walk_distance': 0,
            'ride_distance': 0,
            'move_distance': 0,
            'damage_dealt': 0,
            'kill_place': 0,
            'knock_downs': 0,
            'revives': 0,
            'win_points': 0,
            'match_num': 0,
            'rounds_played': 0,
            'win_num': 0,
            'top_10_num': 0,
            'deaths': 0,
            'round_most_kills': 0,
            'best_rank': 255,
            'longest_kill': 0,
            'max_kill_streaks': 0,
            'most_survival_time': 0,
            'longest_time_survived': 0,
            'most_damage_dealt': 0,
            'most_move_distance': 0
        }

    def group_and_sum(self, pd_list):
        rst = dict()
        for pd in pd_list:
            keys = [               
                '_'.join([pd['server'], '', '', '0']),              
                '_'.join(['', '', '', '0'])
            ]
            for key in keys:             
                if key not in rst:
                    rst[key] = self.default_stats

                server, mode, season, queue_size = key.split('_')
                rst[key]['server'] = server
                rst[key]['mode'] = mode
                rst[key]['season'] = season
                rst[key]['queue_size'] = int(queue_size)
                rst[key]['time_survived'] += pd.get('time_survived')
                rst[key]['vehicle_destroys'] += pd.get('vehicle_destroys')
                rst[key]['heals'] += pd.get('heals')
                rst[key]['weapon_acquired'] += pd.get('weapon_acquired')
                rst[key]['boosts'] += pd.get('boosts')
                rst[key]['kills'] += pd.get('kills')
                rst[key]['assists'] += pd.get('assists')
                rst[key]['road_kills'] += pd.get('road_kills')
                rst[key]['team_kills'] += pd.get('team_kills')
                rst[key]['headshot_kills'] += pd.get('headshot_kills')
                rst[key]['walk_distance'] += pd.get('walk_distance')
                rst[key]['ride_distance'] += pd.get('ride_distance')
                rst[key]['move_distance'] += pd.get('walk_distance') + pd.get('ride_distance')
                rst[key]['damage_dealt'] += pd.get('damage_dealt')
                rst[key]['kill_place'] += pd.get('kill_place')
                rst[key]['knock_downs'] += pd.get('knock_downs')
                rst[key]['revives'] += pd.get('revives')
                rst[key]['avg_rank'] += pd.get('rank')

                rst[key]['match_num'] += 1
                rst[key]['rounds_played'] += 1
                rst[key]['win_num'] += int(pd.get('rank') == 1)
                rst[key]['top_10_num'] += int(pd.get('rank') <= 10)
                rst[key]['deaths'] += int(pd.get('death_type') != 'alive')



                rst[key]['round_most_kills'] = max(rst[key]['round_most_kills'], pd.get('kills'))
                rst[key]['best_rank'] = min(rst[key]['best_rank'], pd.get('rank'))
                rst[key]['longest_kill'] = max(rst[key]['longest_kill'], pd.get('longest_kill'))
                rst[key]['max_kill_streaks'] = max(rst[key]['max_kill_streaks'], pd.get('kill_streaks'))
                rst[key]['most_survival_time'] = max(rst[key]['most_survival_time'], pd.get('time_survived'))
                rst[key]['longest_time_survived'] = max(rst[key]['longest_time_survived'], pd.get('time_survived'))
                rst[key]['most_damage_dealt'] = max(rst[key]['most_damage_dealt'], pd.get('damage_dealt'))
                rst[key]['most_move_distance'] = max(rst[key]['most_move_distance'], pd.get('walk_distance') + pd.get('ride_distance'))


                if 'tmp' not in rst[key]:
                    rst[key]['tmp'] = {
                        'solo_match_num': 0, 'team_match_num': 0, 'solo_kills': 0, 'team_kills': 0, 'solo_heals': 0,
                        'team_heals': 0, 'solo_ride_distance': 0, 'team_ride_distance': 0, 'solo_weapon_acquired': 0,
                        'team_weapon_acquired': 0, 'kill_streaks': 0, 'headshot_kills_rate': 0, 'has_kills_match_num': 0
                    }
                rst[key]['tmp']['headshot_kills_rate'] += pd.get('headshot_kills') / max(1, pd.get('kills'))
                rst[key]['tmp']['has_kills_match_num'] += int(pd.get('kills') > 0)
                rst[key]['tmp']['solo_match_num'] += 1 if pd.get('queue_size') == 1 else 0
                rst[key]['tmp']['team_match_num'] += 1 if pd.get('queue_size') > 1 else 0
                rst[key]['tmp']['solo_kills'] += pd.get('kills') if pd.get('queue_size') == 1 else 0
                rst[key]['tmp']['team_kills'] += pd.get('kills') if pd.get('queue_size') > 1 else 0
                rst[key]['tmp']['solo_heals'] += pd.get('heals') if pd.get('queue_size') == 1 else 0
                rst[key]['tmp']['team_heals'] += pd.get('heals') if pd.get('queue_size') > 1 else 0
                rst[key]['tmp']['solo_ride_distance'] += pd.get('ride_distance') if pd.get('queue_size') == 1 else 0
                rst[key]['tmp']['team_ride_distance'] += pd.get('ride_distance') if pd.get('queue_size') > 1 else 0
                rst[key]['tmp']['solo_weapon_acquired'] += pd.get('weapon_acquired') if pd.get('queue_size') == 1 else 0
                rst[key]['tmp']['team_weapon_acquired'] += pd.get('weapon_acquired') if pd.get('queue_size') > 1 else 0
                rst[key]['tmp']['kill_streaks'] += pd.get('kill_streaks')

        return rst

    def save_player_stats(self, stats):
        where = 'account_name = "%s" AND server = "%s" AND season = "%s" AND mode = "%s" AND queue_size = "%s"'\
                % (stats.get('account_name'), stats.get('server'), stats.get('season'),
                   stats.get('mode'), stats.get('queue_size'))
        logging.info('save stats %s' % where)

        if 'rating_rank' not in stats:
            stats['rating_rank'] = 0
        if 'rating_rank_rate' not in stats:
            stats['rating_rank_rate'] = 0
        if 'rating_trend' not in stats:
            stats['rating_trend'] = ''
        if 'boosts_per_match' not in stats:
            stats['boosts_per_match'] = 0
        s_data = self.mdbcur.fetch_one(table='pubg_player_stats', where=where)
        if type(s_data) == dict:
            self.mdbcur.update(table='pubg_player_stats', where=where, data=stats)
        else:
            self.mdbcur.insert(table='pubg_player_stats', data=stats)

    def handle_bind(self, nickname, pd_list):
        account_list = self.list_by_nickname(nickname=nickname)
        if type(account_list) != list:
            return False

        accountid = ''
        if not pd_list:
            status = 2
        else:
            status = 1
            accountid = pd_list[0].get('account_id')

        for a in account_list:
            update = {
                'status': status,
                'accountid': accountid
            }
            where = "id = %s" % str(a.get('id'))
            self.mdbcur.update(table='game_user_account', data=update, where=where)
        return True

    def list_by_nickname(self, nickname):
        where = "accountname = '%s' AND (status = 2 OR status = 3)" % (nickname)
        return self.mdbcur.fetch_more(table='game_user_account', where=where)

if __name__ == '__main__':

    nickname = ''


    p = PullPubgData()
    rst = p(nickname=nickname)
    if rst:
        ps = PlayerStats()
        ps(nickname=nickname)

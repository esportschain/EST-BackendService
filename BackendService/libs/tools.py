# -*- coding: utf-8 -*-

import os
import logging
import yaml
import requests
import csv
import time
import re
from datetime import datetime, timedelta

from libs.download import Downloader
from binascii import crc32

def list2dict(src_list, field):
    """
    list to ditc
    :param src_list: list
    :param field: item
    :return:
    """
    if not list:
        return {}

    data = {}
    for item in src_list:
        # print
        data[item.get(field)] = item

    return data


def str2time(timestamp, format_str='%Y-%m-%d'):
    """
    format timestamp
    :param timestamp:
    :param format_str:
    :return:
    """
    timestamp = int(timestamp)
    date_dict = datetime.utcfromtimestamp(timestamp)
    return date_dict.strftime(format_str)


def dt_parse2locale(date_string):
    """
    UTC 时间字符串转换为本地时间 datetime
    :param date_string: UTC 时间字符串 2017-12-28T10:59:00+0800
    :return: datetime
    """
    dt_utc = dt_parse2utc(date_string)
    dt_locale = dt_utc_2_locale(dt_utc)
    return dt_locale


def dt_parse2utc(date_string):

    # '2017-12-14T10:23:21+0000'
    date_string = date_string.strip()
    utc_offset_pos = 19   
    utc_offset_hour = int(date_string[utc_offset_pos + 1: utc_offset_pos + 3])
    utc_offset_minute = int(date_string[utc_offset_pos + 3:])

    utc_datetime = datetime.strptime(date_string[0:utc_offset_pos], '%Y-%m-%dT%H:%M:%S')
    if date_string[utc_offset_pos] == '+':
        utc_datetime -= timedelta(hours=utc_offset_hour, minutes=utc_offset_minute)
    elif date_string[utc_offset_pos] == '-':
        utc_datetime += timedelta(hours=utc_offset_minute, minutes=utc_offset_minute)
    return utc_datetime


def dt_utc_2_locale(utc_dt):

    ts = time.time()
    locale_dt_now = datetime.fromtimestamp(ts)
    utc_dt_now = datetime.utcfromtimestamp(ts)
    offset = locale_dt_now - utc_dt_now
    return utc_dt + offset


def join_sort_list(src_list, join_str='_'):


    src_list.sort()
    return join_str.join(src_list)


def download_img(img_url, save_file):
    download = Downloader(delay=5, num_retries=2)
    img_data = download(img_url)


    folder = os.path.dirname(save_file)
    if not os.path.exists(folder):
        os.makedirs(folder)

    img_file = open(save_file, "wb")
    img_file.write(img_data)
    img_file.flush()
    img_file.close()


def get_default_value(table_name, node_name=None):

    def_value = {}
    try:
        def_value = yaml.load(open(os.path.dirname(__file__) + '/../conf/table/' + table_name + '.yaml'))
        if node_name:
            def_value = def_value.get(node_name, {})
    except Exception as e:
        print(e)

    return def_value


def get_conf(conf_name, node_name=None):

    def_value = {}
    try:
        def_value = yaml.load(open(os.path.dirname(__file__) + '/../conf/' + conf_name + '.yaml'))
        if node_name:
            def_value = def_value.get(node_name, {})
    except Exception as e:
        print(e)

    return def_value


def get_conf_by_file_name(file_name, node_name=None):

    def_value = {}
    try:
        def_value = yaml.load(open(file_name))
        if node_name:
            def_value = def_value.get(node_name, {})
    except Exception as e:
        print(e)

    return def_value


def int2bin(n, count=11):
    """returns the binary of integer n, using count number of digits"""

    return "".join([str((n >> y) & 1) for y in range(count-1, -1, -1)])


def send_email_by_sendcloud(receiver=list([]), title='', content='', att_file=None):
   

    conf = get_conf('conf', 'email')
    url = conf.get('url')
    receiver = "%s;%s" % (conf.get('to'), ';'.join(receiver))

    params = {
        "apiUser": conf.get('apiUser'),  
        "apiKey": conf.get('apiKey'),
        "to": receiver,     
        "from": "noreplay@wanplus.com",     
        "fromName": "",
        "subject": title,
        "html": content,
    }

    display_filename = "error_list.csv"
    files = dict({})
    if os.path.exists(att_file):
        files['attachments'] = (display_filename, open(att_file, 'rb'), 'application/octet-stream')
    try:
        rst = requests.post(url, files=files, data=params)
        logging.info('send_email_by_sendcloud rst: %s' % str(rst.text))
    except Exception as e:
        print(e.message)


def write_logs_csv(log_name, message):

    csv_file = file(os.path.dirname(__file__) + '/../data/logs/%s.csv' % log_name, 'a')
    writer = csv.writer(csv_file)
    writer.writerow(message)
    csv_file.close()


def get_avatar(uid, im_size='middle', return_type=0):


    list_size = ['big', 'mid', 'min']
    if im_size not in list_size:
        im_size = 'real'

    uid = int(uid)
    str_uid = "%09d" % uid
    dir1 = str_uid[:3]
    dir2 = str_uid[3:5]
    dir3 = str_uid[5:7]

    if return_type == 1:
   
        return "%s/%s/%s/" % (dir1, dir2, dir3)

    if return_type == 2:
     
        return "%d_%s.jpg" % (uid, im_size)


    return "%s/%s/%s/%d_%s.jpg" % (dir1, dir2, dir3, uid, im_size)


def get_same_str_start_list(str_list):
    
    if not str_list:
        return ''

    str_list.sort()
    same_str = str_list[0]
    for string in str_list:
        for i in range(len(same_str)):
            s = same_str
            for j in range(len(s)):
                if s[j].lower() != string[j].lower():
                    if j == 0:
                        break
                    else:
                        same_str = s[0:j]
                        break

    return same_str


def str2timestamp(date_str, format_str, hour_offset=0):
    time_array = time.strptime(date_str, format_str)
    return int(time.mktime(time_array)) + 3600 * hour_offset


def group_by_key(key, data_list):

    group_dict_by_key = dict({})
    for player_row in data_list:
        # print player_row
        row_key = player_row.get(key)
        if not group_dict_by_key.get(row_key):
            group_dict_by_key[row_key] = list([])

        group_dict_by_key[row_key].append(player_row)

    return group_dict_by_key


def filter_tags(html_str):

    re_cdata = re.compile('//<!\[CDATA\[[^>]*//\]\]>', re.IGNORECASE)       
    re_script = re.compile('<\s*script[^>]*>[^<]*<\s*/\s*script\s*>', re.IGNORECASE)    
    re_style = re.compile('<\s*style[^>]*>[^<]*<\s*/\s*style\s*>', re.IGNORECASE)      
    re_br = re.compile('<br\s*?/?>')            
    re_h = re.compile('</?\w+[^>]*>')           
    re_comment = re.compile('<!--[^>]*-->')     
    s = re_cdata.sub('', html_str)      
    s = re_script.sub('', s)     
    s = re_style.sub('', s)      
    s = re_br.sub('\n', s)       
    s = re_h.sub('', s)          
    s = re_comment.sub('', s)    
   
    blank_line = re.compile('\n+')
    s = blank_line.sub('\n', s)
    return s


def get_pubg_pd_table(nickname):
    tmp = crc32(nickname.lower().encode()) & 0xffffffff
    return 'table_%s' % (tmp % 10)


if __name__ == '__main__':
    s_l = [u'iGaaa', u'IGbbb', u'IGbbb']
    r = get_same_str_start_list(s_l)
    print(r)

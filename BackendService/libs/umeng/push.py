# -*- coding: utf-8 -*-

import json

from libs.umeng.umessage.pushclient import PushClient
from libs.umeng.umessage.iospush import *
from libs.umeng.umessage.androidpush import *

from libs.umeng.umessage.errorcodes import UMPushError, APIServerErrorCode


class Push:

    def __init__(self, app_key, app_master_secret):
        self.app_key = app_key
        self.app_master_secret = app_master_secret

    def send_android_unicast(self, device_token, ticker, title, text, extra, development=1):
        unicast = AndroidUnicast(self.app_key, self.app_master_secret)
        unicast.setDeviceToken(device_token)
        unicast.setTicker(ticker)
        unicast.setTitle(title)
        unicast.setText(text)
        unicast.setPredefinedKeyValue(key='extra', value=extra)
        unicast.goAppAfterOpen()
        unicast.setDisplayType(AndroidNotification.DisplayType.notification)
        if development == 1:
            unicast.setTestMode()
        pushClient = PushClient()
        return pushClient.send(unicast)

    def send_ios_unicast(self, device_token, title, subtitle, body, extra, development=1):
        unicast = IOSUnicast(self.app_key, self.app_master_secret)
        unicast.setDeviceToken(device_token)
        unicast.setAlert({'title': title, 'subtitle': subtitle, 'body': body})
        unicast.setCustomizedField('extra', json.dumps(extra))
        if development == 1:
            unicast.setTestMode()
        pushClient = PushClient()
        ret = pushClient.send(unicast)
        return ret

if __name__ == '__main__':
    p = Push(app_key='', app_master_secret='')
    res = p.send_android_unicast(device_token='',
                                 ticker='ticker test', title='title test', text='text test',
                                 extra={'type': 1, 'data': 'aa'})
    print(res.json())
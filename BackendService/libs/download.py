# -*- coding: utf-8 -*-

import random
import time
from datetime import datetime
import socket
import logging

from urllib import parse
from urllib import request as urequest
# default user_agent
DEFAULT_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) ' \
                'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36'

DEFAULT_DELAY = 5
# retry count
DEFAULT_RETRIES = 1
# timeout
DEFAULT_TIMEOUT = 60


class Downloader:
    def __init__(self, delay=DEFAULT_DELAY, user_agent=None, proxies=None,
                 num_retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT, opener=None, cache=None, headers=None):
        socket.setdefaulttimeout(timeout)
        self.throttle = Throttle(delay)
        self.user_agent = user_agent if user_agent else get_user_agent()
        self.proxies = proxies
        self.num_retries = num_retries
        self.opener = opener
        self.cache = cache
        self.headers = headers

    def __call__(self, url, is_rsp_code=False):

        result = {}
        if self.cache:
            try:
                result = self.cache[url]
            except KeyError:
                # url is not available in cache
                pass
            else:
                if self.num_retries > 0 and 500 <= result['code'] < 600:
                    # server error so ignore result from cache and re-download
                    result = {}
        if not result:
            # result was not loaded from cache so still need to download
            self.throttle.wait(url)
            proxy = random.choice(self.proxies) if self.proxies else None
            headers = dict() if not self.headers else self.headers
            if 'User-agent' not in headers:
                headers['User-agent'] = self.user_agent
            result = self.download(url, headers, proxy=proxy, num_retries=self.num_retries)
            if self.cache:
                # save result to cache
                self.cache[url] = result

        return result.get('html') if not is_rsp_code else result

    def download(self, url, headers, proxy, num_retries, data=None):
        logging.info('start downloading: %s' % url)
        request = urequest.Request(url, data, headers or {})
        opener = self.opener or urequest.build_opener()
        if proxy:
            proxy_params = {parse.urlparse(url).scheme: proxy}
            opener.add_handler(urequest.ProxyHandler(proxy_params))
        try:
            response = opener.open(request)
            html = response.read()
            code = response.code
        except Exception as e:
            logging.error('Download error: %s' % str(e))
            html = ''
            if hasattr(e, 'code'):
                code = e.code
                if num_retries > 0 and 500 <= code < 600:
                    # retry 5XX HTTP errors
                    return self.download(url, headers, proxy, num_retries-1, data)
            else:
                code = None

        return {'html': html, 'code': code}


class Throttle:
    """Throttle downloading by sleeping between requests to same domain
    """
    def __init__(self, delay):
        # amount of delay between downloads for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}

    def wait(self, url):
        """Delay if have accessed this domain recently
        """
        domain = parse.urlsplit(url).netloc
        last_accessed = self.domains.get(domain)
        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (datetime.now() - last_accessed).seconds
            if sleep_secs > 0:
                time.sleep(sleep_secs)
        self.domains[domain] = datetime.now()


def get_user_agent():
    user_agent = [
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; zh-CN; rv:1.9.2.10) Gecko/20100914 Firefox/3.6.10',
        'Dalvik/2.1.0 (Linux; U; Android 5.1.1; vivo Xplay5A Build/LMY47V)',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML,like Gecko) Mobile2.9.7',
        'Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)',
        'Mozilla/5.0 (Windows NT 10.0; WOW64)AppleWebKit/537.36 (KHTML, like Gecko)Chrome/50.0.2661.102 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36',
        'Dalvik/2.1.0 (Linux; U; Android 6.0; Le X620 Build/HEXCNFN5902605181S)',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 8_1_1 like Mac OS X; zh-CN) AppleWebKit/537.51.1 '
        '(KHTML, like Gecko) Mobile/12B435 UCBrowser/11.5.2.961 Mobile  AliApp(TUnionSDK/0.1.15)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2454.101 Safari/538.36',
        'Dalvik/2.1.0 (Linux; U; Android 5.1.1; MX4 Pro Build/LMY48W)',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_2 like Mac OS X) AppleWebKit/603.2.4 (KHTML, '
        'like Gecko) Mobile/14F89 Weibo (iPhone8,1__weibo__7.5.2__iphone__os10.3.2)',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)'
        ' Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2661.102 Safari/517.36',
        'Mozilla/5.0 (Linux; Android 5.1.1; Redmi 3 Build/LMY47V; wv) AppleWebKit/537.36 (KHTML, '
        'like Gecko) Version/4.0 Chrome/45.0.2454.95 Mobile Safari/537.36 WanPlusApp/298 WanPlusApp/298',
        'Dalvik/2.1.0 (Linux; U; Android 6.0; HUAWEI GRA-UL10 Build/HUAWEIGRA-UL10)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/49.0.2623.221 Safari/537.36 SE 2.X MetaSr 1.0',
        'Dalvik/2.1.0 (Linux; U; Android 6.0; Le X620 Build/HEXCNFN5902303291S)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2761.102 Safari/539.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/537.36 (KHTML, '
        'like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:53.0) Gecko/20100101 Firefox/53.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_4) AppleWebKit/603.1.30 (KHTML, like Gecko)'
        ' Version/10.1 Safari/603.1.30'
    ]

    rd_num = random.randint(0, 21)

    return user_agent[rd_num] + ' ' + str(time.clock())

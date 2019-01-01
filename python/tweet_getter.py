'''
provide 1 parent class and 2 child classes to extract tweets from Twitter
Standard search API.

Notes
-----
About Twitter Standard search API
    requests / 15-min (user auth) : 180
    requests / 15-min (app auth) : 450
    not case sensitive
    ';)' and ';(' seems to catch only similar expressions such as ':D' or ':-('
'''


from typing import Tuple
from abc import ABCMeta, abstractmethod  # for using abstrach base class
import datetime, time, sys

import json
from requests_oauthlib import OAuth1Session


class TweetsGetter(object):
    '''
    abstract base class
        - inherited by TweetsGetterBySearh and TweetGetterByUser
    '''
    __metaclass__ = ABCMeta

    def __init__(self):
        with open('./assets/twitter_account.txt', 'r') as f:
            API = dict(line.strip().split('\t')[:2] for line in f)
        self.session = OAuth1Session(
                API['CK'],
                API['CS'],
                API['AT'],
                API['AS'])

    @abstractmethod
    def specifyUrlAndParams(self, keyword):
        '''
        Returns
        -------
        url, params : Tuple[str, dict]
            url to call, parameters

        Notes
        -----
        These methods are abstact method.
        So they are implemented in child classes.
        '''

    @abstractmethod
    def pickupTweet(self, res_text, includeRetweet):
        '''
        res_text からツイートを取り出し、配列にセットして返却
        '''

    @abstractmethod
    def getLimitContext(self, res_text):
        '''
        回数制限の情報を取得 （起動時）
        '''

    def collect(self, total = -1, onlyText = False, includeRetweet = False):
        '''
        ツイート取得を開始する

        Parameters
        ----------
        total : int, default -1
            number of tweets to collect
            if -1, collect unlimited number of tweets (restricted by Twitter
            at some point)
        onlyText : boolean, default False
            T/F to extract only text of tweet
        includeRetweet : boolean, default False
            T/F to allow extracting retweet

        Returns
        -------
        act as a generator of tweet
            generate extracted tweet 1 by 1

        Notes
        -----
        This function sometimes spend minutes waiting api's call-limit to end
        '''

        #----------------
        # check api's call limit
        #----------------
        self.checkLimit()

        #----------------
        # prepare url and params
        #----------------
        url, params = self.specifyUrlAndParams() # url, params: Tuple[str, dict]
        params['include_rts'] = str(includeRetweet).lower()
        # include_rts は statuses/user_timeline のパラメータ。search/tweets には無効

        #----------------
        # get tweet (yield)
        #----------------
        cnt = 0
        unavailableCnt = 0
        while True:
            res = self.session.get(url, params = params)
            if res.status_code == 503:
                # 503 : Service Unavailable
                if unavailableCnt > 10:
                    raise Exception('Twitter API error %d' % res.status_code)

                unavailableCnt += 1
                print ('Service Unavailable 503')
                self.waitUntilReset(time.mktime(datetime.datetime.now().timetuple()) + 30)
                continue

            unavailableCnt = 0

            if res.status_code != 200:
                raise Exception('Twitter API error %d' % res.status_code)

            tweets = self.pickupTweet(json.loads(res.text))
            if len(tweets) == 0:
                # len(tweets) != params['count'] としたいが
                # count は最大値らしいので判定に使えない。
                # ⇒  "== 0" にする
                # https://dev.twitter.com/discussions/7513
                break

            for tweet in tweets:
                if (('retweeted_status' in tweet) and (includeRetweet is False)):
                    pass
                else:
                    if onlyText is True:
                        yield tweet['text']
                    else:
                        yield tweet

                    cnt += 1
                    if cnt % 10000 == 0:
                        print ('{:>8} tweets'.format(cnt))

                    if total > 0 and cnt >= total:
                        return

            params['max_id'] = tweet['id'] - 1

            # ヘッダ確認 （回数制限）
            # X-Rate-Limit-Remaining が入ってないことが稀にあるのでチェック
            if ('X-Rate-Limit-Remaining' in res.headers and 'X-Rate-Limit-Reset' in res.headers):
                if (int(res.headers['X-Rate-Limit-Remaining']) == 0):
                    self.waitUntilReset(int(res.headers['X-Rate-Limit-Reset']))
                    self.checkLimit()
            else:
                print ('not found  -  X-Rate-Limit-Remaining or X-Rate-Limit-Reset')
                self.checkLimit()

    def checkLimit(self):
        '''
        回数制限を問合せ、アクセス可能になるまで wait する
        '''
        unavailableCnt = 0
        while True:
            url = "https://api.twitter.com/1.1/application/rate_limit_status.json"
            res = self.session.get(url)
 
            if res.status_code == 503:
                # 503 : Service Unavailable
                if unavailableCnt > 10:
                    raise Exception('Twitter API error %d' % res.status_code)
 
                unavailableCnt += 1
                print ('Service Unavailable 503')
                self.waitUntilReset(time.mktime(datetime.datetime.now().timetuple()) + 30)
                continue
 
            unavailableCnt = 0
 
            if res.status_code != 200:
                raise Exception('Twitter API error %d' % res.status_code)
 
            remaining, reset = self.getLimitContext(json.loads(res.text))
            if (remaining == 0):
                self.waitUntilReset(reset)
            else:
                break
 
    def waitUntilReset(self, reset):
        '''
        reset 時刻まで sleep
        '''
        seconds = reset - time.mktime(datetime.datetime.now().timetuple())
        seconds = max(seconds, 0)
        print ('\n     =====================')
        print ('     == waiting %d sec ==' % seconds)
        print ('     =====================')
        sys.stdout.flush()
        time.sleep(seconds + 10)  # 念のため + 10 秒
 
    @staticmethod
    def bySearch(keyword):
        '''
        Parameters
        ----------
        keyword : str
            search query `q` given to Twitter search API

        Returns
        -------
        TweetsGetterBySearch : class TweetsGetterBySearch
            has method `collect` which is generator for searched tweets
        '''
        return TweetsGetterBySearch(keyword)
 
    @staticmethod
    def byUser(screen_name):
        return TweetsGetterByUser(screen_name)


class TweetsGetterBySearch(TweetsGetter):
    '''
    child class of TweetsGetter
       search tweets by keyword
    '''
    def __init__(self, keyword):
        super(TweetsGetterBySearch, self).__init__()
        self.keyword = keyword

    def specifyUrlAndParams(self):
        '''
        return url to call and parameters

        Returns
        -------
        url. params : Tuple[str, dict]
            url
                REST API url to call
            params
                q : str
                    utf-8 search query (max 500 chrs)
                count : int
                    number of tweets to return (max 100)
                lang : str (optional)
                    language of tweet
        '''
        url = 'https://api.twitter.com/1.1/search/tweets.json?'
        params = {'q':self.keyword, 'count':100, 'lang':'ja'}
        return url, params
 
    def pickupTweet(self, res_text):
        '''
        extract tweets from res_text and return them as a list

        Parameters
        ----------
        res_text : str
            res (representational state) got from api

        Returns
        -------
        results : List[str]
            list of tweet

        Notes
        -----
        Structures of res_text differ between BySearch and ByUser.
        '''
        results = []
        for tweet in res_text['statuses']:
            results.append(tweet)
 
        return results
 
    def getLimitContext(self, res_text):
        '''
        回数制限の情報を取得 （起動時）
        '''
        remaining = res_text['resources']['search']['/search/tweets']['remaining']
        reset     = res_text['resources']['search']['/search/tweets']['reset']
 
        return int(remaining), int(reset)


class TweetsGetterByUser(TweetsGetter):
    '''
    ユーザーを指定してツイートを取得
    '''
    def __init__(self, screen_name):
        super(TweetsGetterByUser, self).__init__()
        self.screen_name = screen_name
        
    def specifyUrlAndParams(self):
        '''
        呼出し先 URL、パラメータを返す
        '''
        url = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
        params = {'screen_name':self.screen_name, 'count':200}
        return url, params
 
    def pickupTweet(self, res_text):
        '''
        res_text からツイートを取り出し、配列にセットして返却
        '''
        results = []
        for tweet in res_text:
            results.append(tweet)
 
        return results
 
    def getLimitContext(self, res_text):
        '''
        回数制限の情報を取得 （起動時）
        '''
        remaining = res_text['resources']['statuses']['/statuses/user_timeline']['remaining']
        reset     = res_text['resources']['statuses']['/statuses/user_timeline']['reset']
 
        return int(remaining), int(reset)

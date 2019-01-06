"""
module tweet_getter.py

provide convenient classes to extract tweets from Twitter
Standard search API.

Notes
-----
Notes about Twitter Standard search API
    ref -> https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets.html
    requests / 15-min (user auth) : 180
    requests / 15-min (app auth) : 450
    not case sensitive
    ';)' and ';(' seems to catch only similar expressions such as ':D' or ':-('
"""


from abc import ABCMeta, abstractmethod  # for using abstrach base class
import datetime, time, sys

import json
from requests_oauthlib import OAuth1Session


class TweetsGetter(object):
    """
    abstract base class
        - inherited by 2 child class: TweetsGetterBySearh and TweetGetterByUser

    preparation to use
    ------------------
        - place your API-key in `./assets/twitter_account.txt`
          **keep this file SECRET**
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        """
        read API-key information from file
        """
        with open('./assets/twitter_account.txt', 'r') as f:
            API = dict(line.strip().split('\t')[:2] for line in f)
        self.session = OAuth1Session(
                API['CK'],
                API['CS'],
                API['AT'],
                API['AS'])

    @abstractmethod
    def specifyUrlAndParams(self, keyword):
        """
        abstract method to prepare parameters
        (implemented in a child class)

        Returns
        -------
        url, params : Tuple[str, dict]
            url
                REST API url to call
            params
                packed parameters
        """

    @abstractmethod
    def pickupTweet(self, res_text, includeRetweet):
        """
        extract tweets from res_text

        parameters
        ----------
        res_text : str
            RES (representational state) got from REST API

        returns
        -------
        results : list[str]
            list of tweet

        notes
        -----
        structures of res_text differ between bysearch and byuser.
        """

    @abstractmethod
    def getLimitContext(self, res_text):
        """
        extract API's request-limit information from RES text

        Parameters
        ----------
        res_text : json.loads()
            parsed json

        Returns
        -------
        remaining : int
            current remaining request-limit (ex. 180)
        reset : int
            next reset time of requeset-limit (ex. 1403602426)
        """

    def collect(self, total = -1, onlyText = False, includeRetweet = False):
        """
        return 1-by-1 generator of tweet

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
        1-by-1 generator of tweet

        Notes
        -----
        This function sometimes sleep for minutes waiting api's call-limit to end
        """

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
                self.checkLimit()

    def checkLimit(self):
        """
        check current request-limit, and wait until it resets if necessary
        """
        unavailableCnt = 0
        while True:
            url = "https://api.twitter.com/1.1/application/rate_limit_status.json"
            # --   url : URL of `rate limit status API`
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
        """
        sleep until request-limit resets
        """
        seconds = reset - time.mktime(datetime.datetime.now().timetuple())
        seconds = max(seconds, 0)
        print (' -- waiting for %d sec -- ' % seconds)
        sys.stdout.flush()
        time.sleep(seconds + 10)  # 念のため + 10 秒
 
    @staticmethod
    def bySearch(keyword):
        """
        Parameters
        ----------
        keyword : str
            search query `q` given to Twitter search API

        Returns
        -------
        TweetsGetterBySearch : class TweetsGetterBySearch
            has method `collect` which is generator for searched tweets
        """
        return TweetsGetterBySearch(keyword)
 
    @staticmethod
    def byUser(screen_name):
        return TweetsGetterByUser(screen_name)


class TweetsGetterBySearch(TweetsGetter):
    """
    extract tweets searched by selected query
        - child class of TweetsGetter
    """
    def __init__(self, keyword):
        super(TweetsGetterBySearch, self).__init__()
        self.keyword = keyword

    def specifyUrlAndParams(self):
        """
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
        """
        url = 'https://api.twitter.com/1.1/search/tweets.json?'
        params = {'q':self.keyword, 'count':100, 'lang':'ja'}
        return url, params
 
    def pickupTweet(self, res_text):
        """
        extract tweets from res_text and return them as a list

        parameters
        ----------
        res_text : str
            res (representational state) got from api

        returns
        -------
        results : list[str]
            list of tweet

        notes
        -----
        structures of res_text differ between bysearch and byuser.
        """
        results = []
        for tweet in res_text['statuses']:
            results.append(tweet)

        return results

    def getLimitContext(self, res_text):
        """
        extract API's request-limit information from RES text

        Parameters
        ----------
        res_text : json.loads()
            parsed json

        Returns
        -------
        remaining : int
            current remaining request-limit (ex. 180)
        reset : int
            next reset time of requeset-limit (ex. 1403602426)
        """
        remaining = res_text['resources']['search']['/search/tweets']['remaining']
        reset     = res_text['resources']['search']['/search/tweets']['reset']

        return int(remaining), int(reset)


class TweetsGetterByUser(TweetsGetter):
    """
    extract tweets of selected user
        - child class of TweetsGetter
    """
    def __init__(self, screen_name):
        super(TweetsGetterByUser, self).__init__()
        self.screen_name = screen_name

    def specifyUrlAndParams(self):
        """
        return url to call and parameters

        Returns
        -------
        url. params : Tuple[str, dict]
            url
                REST API url to call
            params
                screen_name : str
                    twitter user name (exclude '@')
                count : int
                    number of tweets to return (max 200)
        """
        url = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
        params = {'screen_name':self.screen_name, 'count':200}
        return url, params
 
    def pickupTweet(self, res_text):
        """
        extract tweets from res_text and return them as a list

        parameters
        ----------
        res_text : str
            res (representational state) got from api

        returns
        -------
        results : list[str]
            list of tweet

        notes
        -----
        structures of res_text differ between bysearch and byuser.
        """
        results = []
        for tweet in res_text:
            results.append(tweet)
 
        return results
 
    def getLimitContext(self, res_text):
        """
        extract API's request-limit information from RES text

        Parameters
        ----------
        res_text : json.loads()
            parsed json

        Returns
        -------
        remaining : int
            current remaining request-limit (ex. 180)
        reset : int
            next reset time of requeset-limit (ex. 1403602426)
        """
        remaining = res_text['resources']['statuses']['/statuses/user_timeline']['remaining']
        reset     = res_text['resources']['statuses']['/statuses/user_timeline']['reset']
 
        return int(remaining), int(reset)

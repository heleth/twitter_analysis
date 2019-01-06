#!/usr/bin/env python3
"""
get tweets related to performers of 2018年紅白歌合戦 tweeted during the show

outputs
-------
pickle file of dict[DataFrame] of tweets
    - 1 DataFrame for 1 performer
"""

import datetime
import pickle
from typing import List

import pandas as pd

from python.tweet_getter import TweetsGetter


# ------------------------------------------------------------------------
## prepare conditions
# ------------------------------------------------------------------------
with open('txt/performers_with_query_utf16.txt', 'r', encoding='utf-16') as f:
    header_trash = f.readline()
    performers = dict(line.strip('\n').split('\t', 1) for line in f)
    PERFORMERS = {k:[v for v in v.split('\t') if v != ''] for (k,v) in performers.items()}

dict_tweets = {}
for i, [performer, names] in enumerate(PERFORMERS.items()):
    # ------------------------------------------------------------------------
    ## define class to get tweets
    # ------------------------------------------------------------------------
    query_names = 'OR'.join(['"'+name+'"' for name in names])
    TOTAL = [1000000, 5][0]
    PERIOD = [
        {'since':'2018-12-31_19:15:00_JST', 'until':'2018-12-31_23:45:00_JST'},
        {'since':'2018-12-31_19:15:00_JST', 'until':'2018-12-31_19:30:00_JST'},
        ][0]
    query_full = u'''{} AND
    -filter:retweets AND
    -filter:replies AND
    since:{}
    until:{}'''.format(query_names, PERIOD['since'], PERIOD['until'])
#    query_full = u'''{} AND "紅白" AND
#    -filter:retweets AND
#    -filter:replies AND
#    since:{}
#    until:{}'''.format(query_names, PERIOD['since'], PERIOD['until'])
    getter = TweetsGetter.bySearch(query_full)

    # ------------------------------------------------------------------------
    ## get tweets
    # ------------------------------------------------------------------------
    print('performer : {:<20}, name : {:<10}'.format(performer, query_names))
    l_tweets_time = []
    l_tweets_text = []
    for j, tweet in enumerate(getter.collect(total = TOTAL)):
        l_tweets_time.append(tweet['created_at'])
        l_tweets_text.append(tweet['text'])
        # --   created_at is GST
        if (False):
            print ('-- {:>4} of {:>4} --\n    id={}, time={}GST, by {}\n    {}'.format(
                j+1,
                TOTAL,
                tweet['id'],
                tweet['created_at'],
                '@'+tweet['user']['screen_name'],
                tweet['text'].split('\n', 1)[:]))
    print('    {:>8} tweets colloected.'.format(j+1))

    dict_tweets[performer] = pd.concat([
        pd.Series(pd.to_datetime(l_tweets_time)).rename('tweet_time').dt.tz_localize('utc')\
                .dt.tz_convert('Asia/Tokyo'),
        # -- `l_tweets_time` is recognized as timezone='UTC'
        # -- so, need to be converted to 'Asia/Tokyo' local time
        pd.Series(l_tweets_text).rename('tweet_text'),], axis=1)

# ------------------------------------------------------------------------
## store tweet-data in pickle file
# ------------------------------------------------------------------------
filename = 'txt/dict_tweets_' + datetime.datetime.now().strftime('%d%H%M') +'.pickle'
with open(filename, 'wb') as f:
    pickle.dump(dict_tweets, f)
print('{} created.'.format(filename))


# ------------------------------------------------------------------------
## test code
# ------------------------------------------------------------------------

# -- test from sample code 1
#getter = TweetsGetter.bySearch(u'("マジカルラブリー" OR "とろサーモン") AND -filter:retweets AND \
#        -filter:replies AND until:2018-12-31_22:05:00_JST')
#TOTAL = 10
#created_at: List[str] = []
#text: List[str] = []
#for tweet in getter.collect(total = TOTAL):
#    created_at.append(tweet['created_at'])
#    # --   tweet['created_at'] is seen to be GST
#    text.append(tweet['text'])
#    if (True):
#        print ('-- {:>4} of {:>4} --\n    {} {} {}\n    {}'.format(
#            len(text),
#            TOTAL,
#            tweet['id'],
#            tweet['created_at'],
#            '@'+tweet['user']['screen_name'],
#            tweet['text'].split('\n', 1)[:]))
#
# -- test from sample code 2
#getter = TweetsGetter.byUser('AbeShinzo')
#TOTAL = 2
#created_at: List[str] = []
#text: List[str] = []
#for tweet in getter.collect(total = TOTAL):
#    created_at.append(tweet['created_at'])
#    # --   tweet['created_at'] is seen to be GST
#    text.append(tweet['text'])
#    if (True):
#        print ('-- {:>4} of {:>4} --\n    {} {} {}\n    {}'.format(
#            len(text),
#            TOTAL,
#            tweet['id'],
#            tweet['created_at'],
#            '@'+tweet['user']['screen_name'],
#            tweet['text'].split('\n', 1)[:]))


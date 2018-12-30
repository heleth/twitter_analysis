#!/usr/bin/env python3

from typing import List

import pandas as pd
#from dateutil.parser import parse  # from sample code

from python.tweet_getter import TweetsGetter

# ------------------------------------------------------------------------
## define class to get tweets
# ------------------------------------------------------------------------
# -- test from sample code 1
#getter = TweetsGetter.bySearch(u'"マジカルラブリー" AND -filter:retweets AND \
#        -filter:replies AND until:2018-12-31_22:05:00_JST')
#TOTAL = 2

# -- test from sample code 2
#getter = TweetsGetter.byUser('AbeShinzo')
#TOTAL = 2

# -- test 1
WORD = '東京'
PERIOD = [
    {'since':'2018-12-31_00:00:00_JST', 'until':'2018-12-31_23:59:59_JST'},
    {'since':'2018-12-31_00:00:00', 'until':'2018-12-31_23:59:59'},
    ][0]
# --   '_JST' is correctly recognized by API
QUERY = u'''"{}" AND
-filter:retweets AND
-filter:replies AND
since:{}
until:{}'''.format(WORD, PERIOD['since'], PERIOD['until'])
getter = TweetsGetter.bySearch(QUERY)
TOTAL = 2
print(QUERY)

# ------------------------------------------------------------------------
## get tweets
# ------------------------------------------------------------------------
created_at: List[str] = []
text: List[str] = []
for tweet in getter.collect(total = TOTAL):
    created_at.append(tweet['created_at'])
    # --   tweet['created_at'] is seen to be GST
    text.append(tweet['text'])
    if (True):
        print ('-- {:>4} of {:>4} --\n    {} {} {}\n    {}'.format(
            len(text),
            TOTAL,
            tweet['id'],
            tweet['created_at'],
            '@'+tweet['user']['screen_name'],
            tweet['text'].split('\n', 1)[0]))



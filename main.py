#!/usr/bin/env python3

from typing import List
import datetime

import pandas as pd
#from dateutil.parser import parse  # from sample code

from python.tweet_getter import TweetsGetter


# ------------------------------------------------------------------------
## prepare conditions
# ------------------------------------------------------------------------
with open('txt/performers_with_query_utf16.txt', 'r', encoding='utf-16') as f:
    header_trash = f.readline()
    performers = dict(line.strip('\n').split('\t', 1) for line in f)
    PERFORMERS = {k:[v for v in v.split('\t') if v != ''] for (k,v) in performers.items()}

tweets_by_performer = pd.DataFrame(0,
        index=range(len(PERFORMERS)),
        columns=['performer', 'cnt','tweets'])
for i, [performer, queries] in enumerate(PERFORMERS.items()):
    if(False):
        if (i>3):
            break
    created_at: List[str] = []
    text: List[str] = []

    for query in queries:
        print('performer : {:<20}, query : {:<10}'.format(performer, query))
        # ------------------------------------------------------------------------
        ## define class to get tweets
        # ------------------------------------------------------------------------
        TOTAL = 100
        WORD = query
        PERIOD = [
            {'since':'2018-12-31_19:15:00_JST', 'until':'2018-12-31_23:45:00_JST'},
            {'since':'2018-12-31_19:15:00_JST', 'until':'2018-12-31_19:30:00_JST'},
            {'since':'2018-12-31_00:00:00_JST', 'until':'2018-12-31_23:59:59_JST'},
            {'since':'2018-12-31_00:00:00', 'until':'2018-12-31_23:59:59'},  # GST
            ][1]
        # --   '_JST' is correctly recognized by API
        QUERY = u'''"{}" AND
        -filter:retweets AND
        -filter:replies AND
        since:{}
        until:{}'''.format(WORD, PERIOD['since'], PERIOD['until'])
        getter = TweetsGetter.bySearch(QUERY)

        # ------------------------------------------------------------------------
        ## get tweets
        # ------------------------------------------------------------------------
        for tweet in getter.collect(total = TOTAL):
            created_at.append(tweet['created_at'])
            # --   tweet['created_at'] is seen to be GST
            text.append(tweet['text'])
            if (False):
                print ('-- {:>4} of {:>4} --\n    {} {} {}\n    {}'.format(
                    len(text),
                    TOTAL,
                    tweet['id'],
                    tweet['created_at'],
                    '@'+tweet['user']['screen_name'],
                    tweet['text'].split('\n', 1)[:]))

    tweets_by_performer.loc[i, 'performer'] = performer
    tweets_by_performer.loc[i, 'cnt'] = len(text)
    tweets_by_performer.loc[i,'tweets'] = '|'.join(text).replace('\n', ' ')

filename = 'txt/tweets_by_performer_' + datetime.datetime.now().strftime('%H%M') + '.tsv'
print('{} created.'.format(filename))
tweets_by_performer.to_csv(filename, sep='\t')

# -- test from sample code 1
#getter = TweetsGetter.bySearch(u'"マジカルラブリー" AND -filter:retweets AND \
#        -filter:replies AND until:2018-12-31_22:05:00_JST')
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


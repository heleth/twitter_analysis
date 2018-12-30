#!/usr/bin/env python3

from python import tweet_getter as tg

# from dateutil.parser import parse



getter = tg.TweetsGetter.bySearch(u'"マジカルラブリー" AND -filter:retweets AND\
        -filter:replies AND until:2018-12-31_22:05:00_JST')

#getter = tg.TweetsGetter.byUser('AbeShinzo')

print(getter)

for tweet in getter.collect(total=2):
    print(tweet['text'])

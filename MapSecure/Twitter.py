#!/usr/bin/env python
# -- coding: utf-8 --

from TwitterScrapperRoHec import query_tweets

if __name__ == '__main__':
    #Or save the retrieved tweets to file:
    file = open('output.txt', 'w')
    for tweet in query_tweets("(asalto OR acoso OR violencia) AND CDMX", 50):
        try:
            file.write(tweet.text + "\n")
        except:
            continue
    file.close()

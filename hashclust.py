#!/usr/bin/env python

"""
    hashclust.py
    
    Ex:
        time cat data/tmp.jl | ./hashtagclust.py
        time cat data/train.txt | ./hashtagclust.py
"""

# !! Dealing w/ out-of-order messages
# !! "Message count" is currently returning hashtag counts.  Fine w/ me, but may need to be changed.
# !! Retweets sortof mess this up
# !! How to choose number of clusters
# !! Chokes on 1 labels

import sys
import codecs
import ultrajson as json
from datetime import datetime

import twutils
from buffer_runner import BufferRunner

from hashtag_clusterer import HashtagClusterer
from hashtag_publisher import HashtagPublisher
from hashtag_model import HashtagSupervised

sys.stdin = codecs.getwriter("utf-8")(sys.stdin)

import logging
logging.basicConfig(
    filename='./logs/log-%s' % datetime.now().strftime('%Y%m%d%H%M%S'), 
    format='%(asctime)s %(message)s', 
    level=logging.INFO
)

# --
# Model

# def clean_obj(x):
#     for campaign_tag in x['campaign_tags']:
#         yield {
#             'lang' : x['doc']['lang'],
#             'campaignId': campaign_tag['campaignId'],
#             'timestamp': x['norm']['timestamp'],
#             'clean_body': twutils.clean_tweet(x['norm']['body'], polyglot=True),
#         }

def clean_obj(x):
    lang = x['lang']
    if lang != 'en':
        clean_body = twutils.clean_tweet(x['clean_body'])
    else:
        clean_body = twutils.clean_tweet(x['clean_body'], lang=x['lang'])
    
    for campaign_tag in x['campaign_tags']:
        yield {
            'campaignId': campaign_tag['campaignId'],
            'timestamp': x['timestamp'],
            'clean_body': clean_body,
            'lang' : lang,
        }

def clean_gen(gen):
    for i,x in enumerate(gen):
        try:
            for y in clean_obj(json.loads(x)):
                yield y
        except:
            print logging.warning("unicode error: %s" % json.dumps(x))

# --
# Run

if __name__ == "__main__":
    config = json.load(open('config.json'))
    logging.info(str(config))
    
    brs = {}
    for i, obj in enumerate(clean_gen(sys.stdin)):
        if not obj:
            continue
        
        if not i % 1000:
            logging.info("Processed %d records" % i)
        
        cid = str(obj['campaignId'])
        
        # Create campaign model, if doesn't exist
        if not brs.get(cid):
            logging.info('creating %s' % cid)
            cid_model = HashtagSupervised('./output/%s' % cid, config)
            brs[cid] = BufferRunner(cid_model.run, **config['buffer'])
        
        # Add message
        brs[cid].add({
            "timestamp" : obj['timestamp'],
            "content" : obj['clean_body']
        })

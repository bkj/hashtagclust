#!/usr/bin/env python

"""
    hashclust.py
    
    Ex:
        time cat data/tmp.jl | ./__main__.py
        time cat data/train.txt | ./__main__.py

    Issues:
        !! Dealing w/ out-of-order messages
        !! "Message count" is currently returning hashtag counts.  Fine w/ me, but may need to be changed.
        !! Retweets sortof mess this up
        !! How to choose number of clusters
        !! Restarting after failure -- should reload from output files
"""


import sys
import json
import codecs
import ultrajson as json
from datetime import datetime

import twutils
from buffer_runner import BufferRunner
from model import HashtagSupervised
from hashtag_io import clean_gen

sys.stdin = codecs.getwriter("utf-8")(sys.stdin)

import logging
logging.basicConfig(
    filename='./logs/log-%s' % datetime.now().strftime('%Y%m%d%H%M%S'), 
    format='%(asctime)s %(message)s', 
    level=logging.INFO
)

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
        
        # Get campaignId
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

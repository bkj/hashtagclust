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

import os
import sys
import json
import codecs
import ultrajson as json
from datetime import datetime

import twutils
from hashclust import BufferRunner, HashtagSupervised
from hashclust.hashtag_io import clean_gen

sys.stdin = codecs.getwriter("utf-8")(sys.stdin)

import logging

# --
# Run

def main():
    
    if len(sys.argv) == 1:
        raise Exception('need to pass config file')
    
    config = json.load(open(sys.argv[1]))
    
    if not os.path.exists(config['log_dir']):
        os.mkdir(config['log_dir'])
    
    log_file = os.path.join(config['log_dir'], 'log-%s' % datetime.now().strftime('%Y%m%d%H%M%S'))
    print "hashclust logging to: %s" % log_file
    logging.basicConfig(
        filename=log_file, 
        format='%(asctime)s %(message)s', 
        level=logging.INFO
    )
    
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

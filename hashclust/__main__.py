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
import logging
from datetime import datetime

import twutils
import hashclust.io as hio
from hashclust import DiskBufferRunner, HashtagSupervised

sys.stdin = codecs.getwriter("utf-8")(sys.stdin)

def main():
    
    if len(sys.argv) == 1:
        raise Exception('need to pass config file')
    
    config = json.load(open(sys.argv[1]))
    
    for path in [config['log_dir'], config['output_dir'], config['data_dir']]:
        if not os.path.exists(path):
            os.mkdir(path)
    
    log_file = os.path.join(config['log_dir'], 'log-%s' % datetime.now().strftime('%Y%m%d%H%M%S'))
    print "hashclust logging to: %s" % log_file
    logging.basicConfig(
        filename=log_file, 
        format='%(asctime)s %(message)s', 
        level=logging.INFO
    )
    
    logging.info(str(config))
    
    brs = {}
    for i, obj in enumerate(hio.clean_gen(sys.stdin)):
        
        if not obj:
            continue
        
        if not i % 1000:
            logging.info("Processed %d records" % i)
        
        # Get campaignId
        cid = str(obj['campaignId'])
        
        # Create campaign model, if doesn't exist
        if not brs.get(cid):
            logging.info('creating %s' % cid)
            
            model_path = os.path.join(config['output_dir'], cid)
            data_path = os.path.join(config['data_dir'], cid)
            
            model = HashtagSupervised(model_path, config)
            brs[cid] = DiskBufferRunner(
                model.run,
                filename=data_path,
                **config['buffer']
            )
        
        # Add message
        brs[cid].add(**{
            "obj_memory" : obj['timestamp'],
            "obj_disk" : obj['clean_body']
        })

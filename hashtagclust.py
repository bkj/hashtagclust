#!/usr/bin/env python

# !! Tokenizing other languages

import os
import sys
import codecs
import tempfile
import ultrajson as json

import fasttext as ft
import twutils

from buffer_runner import BufferRunner

sys.stdin  = codecs.getreader("utf-8")(sys.stdin)
sys.stdout = codecs.getwriter("utf-8")(sys.stdout)

# --
# Functions

class UnicodeNamedTemporaryFile:
    def __init__(self):
        f = tempfile.NamedTemporaryFile(delete=False)
        self.name = f.name
        f.close()
    
    def write(self, x):
        tmp = codecs.open(self.name, 'w', encoding='utf-8')
        tmp.write(x)
        tmp.flush()
    
    def close(self):
        os.unlink(self.name)

# --
# Model

class BufferedSupervised:
    
    def __init__(self, output):
        self.output = output
        self.ft_args = {
            "label_prefix" : "#",
            "minn" : 0,
            "maxn" : 0,
            "min_count" : 10,
            "min_count_label" : 1
        }
        self.counter = 0
    
    def run(self, data):
        # Write list to file
        tmp = UnicodeNamedTemporaryFile()
        tmp.write('\n'.join(data))
        
        # Train model
        model = ft.supervised(
            tmp.name, 
            '%s-%d' % (self.output, self.counter), 
            **self.ft_args
        )
        self.counter += 1
        
        # Close file
        tmp.close()
        return model


def clean_obj(x):
    campaign_tags = x['campaign_tags']
    lang = x['doc']['lang']
    if lang == 'en':
        for campaign_tag in campaign_tags:
            yield {
                'campaignId' : campaign_tag['campaignId'],
                'timestamp'  : x['norm']['timestamp'],
                'clean_body' : twutils.clean_tweet(x['norm']['body']),
            } 

def clean_gen(gen):
    for x in gen:
        for y in clean_obj(json.loads(x)):
            yield y

# --
# Run

if __name__ == "__main__":
    brs = {}
    for obj in clean_gen(sys.stdin):
        cid = str(obj['campaignId'])
        
        # Create campaign model, if doesn't exist
        if not brs.get(cid):
            print >> sys.stderr, 'creating %s' % cid
            cid_model = BufferedSupervised('./output/%s' % cid)
            brs[cid] = BufferRunner(cid_model.run, maxlen=1000)
        
        # Add message
        brs[cid].add(obj['clean_body'])




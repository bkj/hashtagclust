#!/usr/bin/env python

# !! Tokenizing other languages

import os
import sys
import codecs
import tempfile
import threading
import ultrajson as json

import fasttext as ft
import twutils

from buffer_runner import BufferRunner

sys.stdin = codecs.getreader("utf-8")(sys.stdin)
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

from time import sleep


class BufferedSupervised:

    def __init__(self, output, verbose=True):
        self.output = output
        self.ft_args = {
            "label_prefix": "#",
            "minn": 0,
            "maxn": 0,
            "min_count": 10,
            "min_count_label": 1,
            "epoch": 40,
            "loss": "ns",
            "neg": 100,
            "dim": 100,
            "thread": 5,
            "verbose": 0
        }
        self.counter = 0
        self.verbose = verbose

    def run(self, data):
        self.counter += 1
        pid = os.fork()
        if pid == 0:
            self.run_(data)
            os._exit(0)

    def run_(self, data):
        # Write list to file
        tmp = UnicodeNamedTemporaryFile()
        tmp.write('\n'.join(data))

        # Train model
        if self.verbose:
            print "\nRunning model for: %s (%d)" % (self.output, self.counter)

        model = ft.supervised(
            tmp.name,
            '%s-%d' % (self.output, self.counter),
            **self.ft_args
        )
        
        # Close file
        tmp.close()


def clean_obj(x):
    campaign_tags = x['campaign_tags']
    lang = x['doc']['lang']
    if lang == 'en':
        for campaign_tag in campaign_tags:
            yield {
                'campaignId': campaign_tag['campaignId'],
                'timestamp': x['norm']['timestamp'],
                'clean_body': twutils.clean_tweet(x['norm']['body']),
            }


def clean_gen(gen):
    for x in gen:
        for y in clean_obj(json.loads(x)):
            yield y

# --
# Run

if __name__ == "__main__":
    brs = {}
    for i, obj in enumerate(clean_gen(sys.stdin)):
        if not i % 100:
            print i

        cid = str(obj['campaignId'])

        # Create campaign model, if doesn't exist
        if not brs.get(cid):
            print >> sys.stderr, 'creating %s' % cid
            cid_model = BufferedSupervised('./output/%s' % cid)
            brs[cid] = BufferRunner(
                cid_model.run, maxlen=1e6, count_interval=1000)

        # Add message
        brs[cid].add(obj['clean_body'])

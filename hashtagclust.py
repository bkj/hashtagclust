#!/usr/bin/env python

"""
    hashtagclust.py
    
    Ex:
        time cat data/tmp.jl | ./hashtagclust.py
        time cat data/train.txt | ./hashtagclust.py
"""

# !! Tokenizing other languages
# !! Dealing w/ out-of-order messages
# !! "Message count" is currently returning hashtag counts.  Fine w/ me, but may need to be changed.

import os
import sys
import logging
import codecs
import tempfile
import numpy as np
import ultrajson as json

from datetime import datetime
from uuid import uuid1
from kafka import KafkaProducer
from scipy.cluster import hierarchy
from collections import Counter

import twutils
import fasttext as ft
from buffer_runner import BufferRunner

sys.stdin = codecs.getreader("utf-8")(sys.stdin)
sys.stdout = codecs.getwriter("utf-8")(sys.stdout)

logging.basicConfig(filename='./logs/log-%s' % datetime.now().strftime('%Y%m%d%H%M%S'), format='%(asctime)s %(message)s', level=logging.INFO)

# --
# Functions

class UnicodeNamedTemporaryFile:
    
    def __init__(self, prefix):
        f = tempfile.NamedTemporaryFile(prefix='hc-%s' % prefix, delete=False)
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

class HashtagPublisher:
    
    def __init__(self, config):
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = config['topic']
        
    def __call__(self, campaignId, data, time_interval, counter):
        clusters = np.unique(data['clusters'])
        for cluster in clusters:
            sel = data['clusters'] == cluster
            hashtags = data['labs'][sel]
            self.producer.send(self.topic, {
                "uid": str(uuid1()),
                "campaignId": campaignId,
                
                "startDate": time_interval[0],
                "endDate": time_interval[1],
                
                "label": hashtags[0],
                "hashtags": list(hashtags),
                
                "topicMessageCount": data['lab_counts'][sel].sum(),
                "totalMessageCount": data['lab_counts'].sum(),
                
                "newsEventIds": None,
                "location": None,
                "keywords": None,
                "urls": None,
                "photos": None,
                "importanceScore": None,
                
                "counter" : counter
            })
        
        self.producer.flush()


class HashtagClusterer:
    
    def __init__(self, config):
        self.config = config
    
    def __call__(self, data):
        """ Perform average linkage hierarchical clustering on embeddings """
        labs = data['labs'][:self.config['n_most_common']]
        lab_counts = data['lab_counts'][:self.config['n_most_common']]
        
        nvecs = data['vecs'][:self.config['n_most_common']]
        nvecs /= np.sqrt((nvecs ** 2).sum(axis=1, keepdims=True))
        link = hierarchy.linkage(nvecs, method='average', metric='cosine')
        
        return {
            "labs"       : labs,
            "lab_counts" : lab_counts,
            "clusters"   : hierarchy.cut_tree(link, n_clusters=5).squeeze()
        }


class HashtagSupervised:
    
    def __init__(self, output, config):
        self.output  = output
        self.campaignId = os.path.basename(output)
        
        self.config = config
        self.counter = 0
    
    def run(self, data):
        self.counter += 1
        pid = os.fork()
        if pid == 0:
            model_name = '%s-%d' % (self.output, self.counter)
            self.clusterer = HashtagClusterer(self.config['clusterer'])
            self.publisher = HashtagPublisher(self.config['publisher'])
            
            # Train model
            logging.info("Starting: %s" % model_name)
            
            self.model = self.train(model_name, data)
            label_vectors = self.get_label_vectors()
            clusters = self.clusterer(label_vectors)
            self.publisher(self.campaignId, clusters, self.time_interval, self.counter)
            
            logging.info("Done: %s" % model_name)
            
            os._exit(0)
    
    def train(self, model_name, data):
        
        content = [d['content'] for d in data]
        timestamps = [d['timestamp'] for d in data]
        
        self.time_interval = [min(timestamps), max(timestamps)]
        
        # !! Would be better if we could keep in memory, obviously
        tmp = UnicodeNamedTemporaryFile(os.path.basename(model_name))
        tmp.write('\n'.join(content))
        
        model = ft.supervised(
            tmp.name,
            model_name,
            **self.config['fasttext']
        )
        
        # tmp.close()
        return model
    
    def get_label_vectors(self):
        vecs = map(self.model._model.dict_get_label_vector, range(self.model._model.dict_nlabels()))    
        return { 
            "labs"       : np.array(self.model.labels), 
            "vecs"       : np.vstack(vecs),
            "lab_counts" : np.array(self.model._model.dict_get_label_counts()),
        }

def clean_obj(x):
    for campaign_tag in x['campaign_tags']:
        yield {
            'lang' : x['doc']['lang'],
            'campaignId': campaign_tag['campaignId'],
            'timestamp': x['norm']['timestamp'],
            'clean_body': twutils.clean_tweet(x['norm']['body']),
        }


def clean_gen_ist(gen):
    counter = Counter()
    for i,x in enumerate(gen):
        try:
            for y in clean_obj(json.loads(x)):
                if y['lang'] == 'en':
                    yield y
                counter[y['lang']] += 1
                
            if not i % 1000:
                logging.info('clean_gen_ist : %s' % str(counter))
        except:
            pass


def clean_gen_local(gen):
    for i,x in enumerate(gen):
        try:
            obj = json.loads(x)
            yield {
                "campaignId" : 'london-test',
                "timestamp" : obj['postedTime'],
                "clean_body" : obj['clean_body']
            }
        except:
            pass

# --
# Run

clean_gen = clean_gen_ist

if __name__ == "__main__":
    config = json.load(open('config.json'))
    logging.info(str(config))
    
    brs = {}
    for i, obj in enumerate(clean_gen(sys.stdin)):
        if not i % 100:
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

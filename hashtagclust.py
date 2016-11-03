#!/usr/bin/env python

"""
    hashtagclust.py
    
    Ex:
        time cat data/tmp.jl | ./hashtagclust.py
        time cat data/train.txt | ./hashtagclust.py
"""

# !! Tokenizing other languages
# !! Dealing w/ out-of-order messages

import os
import sys
import codecs
import tempfile
import numpy as np
import ultrajson as json

from uuid import uuid1
from kafka import KafkaProducer
from scipy.cluster import hierarchy

import twutils
import fasttext as ft
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

class HashtagPublisher:
    
    def __init__(self, config):
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = config['topic']
        
    def __call__(self, campaignId, data, time_interval):
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
            })
        
        self.producer.flush()


class HashtagClusterer:
    
    def __init__(self, config):
        self.config = config
    
    def __call__(self, data):
        """ Perform average linkage hierarchical clustering on embeddings """
        nvecs = data['vecs'][:self.config['n_most_common']]
        nvecs /= np.sqrt((nvecs ** 2).sum(axis=1, keepdims=True))
        link = hierarchy.linkage(nvecs, method='average', metric='cosine')
        return {
            "labs"       : data['labs'][:self.config['n_most_common']],
            "lab_counts" : data['lab_counts'][:self.config['n_most_common']],
            "clusters"   : hierarchy.cut_tree(link, n_clusters=self.config['n_clusters']).squeeze()
        }


class HashtagSupervised:

    def __init__(self, output, config, verbose=True):
        self.output  = output
        self.campaignId = os.path.basename(output)
        
        self.config = config
        self.counter = 0
        self.verbose = verbose
    
    def run(self, data):
        self.counter += 1
        pid = os.fork()
        if pid == 0:
            model_name = '%s-%d' % (self.output, self.counter)
            self.clusterer = HashtagClusterer(self.config['clusterer'])
            self.publisher = HashtagPublisher(self.config['publisher'])
            
            # Train model
            if self.verbose:
                print "\n Starting: %s" % model_name
            
            self.model = self.train(model_name, data)
            
            label_vectors = self.get_label_vectors()
            clusters = self.clusterer(label_vectors)
            self.publisher(self.campaignId, clusters, self.time_interval)
            
            if self.verbose:
                print "\n Done: %s" % model_name
            
            os._exit(0)
    
    def train(self, model_name, data):
        
        content = [d['content'] for d in data]
        timestamps = [d['timestamp'] for d in data]
        
        self.time_interval = [min(timestamps), max(timestamps)]
        
        # !! Would be better if we could keep in memory, obviously
        tmp = UnicodeNamedTemporaryFile()
        tmp.write('\n'.join(content))
        
        model = ft.supervised(
            tmp.name,
            model_name,
            **self.config['fasttext']
        )
        
        tmp.close()
        return model
    
    def get_label_vectors(self):
        vecs = map(self.model._model.dict_get_label_vector, range(self.model._model.dict_nlabels()))    
        return { 
            "labs"       : np.array(self.model.labels), 
            "vecs"       : np.vstack(vecs),
            "lab_counts" : np.array(self.model._model.dict_get_label_counts()),
        }

# def clean_obj(x):
#     campaign_tags = x['campaign_tags']
#     lang = x['doc']['lang']
#     if lang == 'en':
#         for campaign_tag in campaign_tags:
#             yield {
#                 'campaignId': campaign_tag['campaignId'],
#                 'timestamp': x['norm']['timestamp'],
#                 'clean_body': twutils.clean_tweet(x['norm']['body']),
#             }


# def clean_gen(gen):
#     for x in gen:
#         for y in clean_obj(json.loads(x)):
#             yield y

def clean_gen(gen):
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

if __name__ == "__main__":
    config = json.load(open('config.json'))
    print(config)
    
    brs = {}
    for i, obj in enumerate(clean_gen(sys.stdin)):
        if not i % 25000:
            print i
        
        cid = str(obj['campaignId'])
        
        # Create campaign model, if doesn't exist
        if not brs.get(cid):
            print >> sys.stderr, 'creating %s' % cid
            cid_model = HashtagSupervised('./output/%s' % cid, config)
            brs[cid] = BufferRunner(cid_model.run, **config['buffer'])
        
        # Add message
        brs[cid].add({
            "timestamp" : obj['timestamp'],
            "content" : obj['clean_body']
        })

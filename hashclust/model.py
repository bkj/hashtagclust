import os
import logging
import numpy as np
import fasttext as ft

from clusterer import HashtagClusterer
from publisher import HashtagPublisher
from utils import UnicodeNamedTemporaryFile

class HashtagSupervised:
    
    def __init__(self, output, config):
        self.output  = output
        self.campaignId = os.path.basename(output)
        
        self.config = config
        self.counter = 0
    
    def run(self, data):
        self.counter += 1
        
        p1=os.fork()
        if p1 != 0:
            os.waitpid(p1, 0)
        else:
            p2=os.fork()
            if p2 != 0:
                os._exit(0)
            else:
                self.run_fork(data)
                os._exit(0)
    
    def run_fork(self, data):
        model_name = '%s-%d' % (self.output, self.counter)
        self.clusterer = HashtagClusterer(self.config['clusterer'])
        self.publisher = HashtagPublisher(self.config['publisher'])
        
        # Train model
        logging.info("Starting: %s (%d records)" % (model_name, len(data)))
        self.model = self.train(model_name, data)
        if not self.model:
            logging.info("Failed: %s" % model_name)
            return
        
        label_vectors = self.get_label_vectors()
        if label_vectors:
            logging.info("Clustering: %s" % model_name)
            clusters = self.clusterer(label_vectors)
            logging.info("Publishing: %s" % model_name)
            self.publisher(self.campaignId, clusters, self.time_interval, self.counter, len(data))
            logging.info("Published: %s" % model_name)
        else:
            logging.info("No labels for: %s" % model_name)
    
    def train(self, model_name, data):
        
        content = [d['content'] for d in data]
        timestamps = [d['timestamp'] for d in data]
        
        self.time_interval = [min(timestamps), max(timestamps)]
        
        # !! Would be better if we could keep in memory, obviously
        tmp = UnicodeNamedTemporaryFile(os.path.basename(model_name))
        tmp.write('\n'.join(content))
        
        try:
            model = ft.supervised(
                tmp.name,
                model_name,
                **self.config['fasttext']
            )
            # tmp.close()
            return model
        except:
            # tmp.close()
            return None
    
    def get_label_vectors(self):
        vecs = map(self.model._model.dict_get_label_vector, range(self.model._model.dict_nlabels()))
        if len(vecs):
            return { 
                "labs"       : np.array(self.model.labels), 
                "vecs"       : np.vstack(vecs),
                "lab_counts" : np.array(self.model._model.dict_get_label_counts()),
            }
        else:
            return None

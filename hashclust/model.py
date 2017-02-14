import os
import logging
import numpy as np
import fasttext as ft

from datetime import datetime

from clusterer import HashtagClusterer
from publisher import HashtagPublisher

class HashtagSupervised:

    def __init__(self, model_path, config):
        self.model_path = model_path
        self.campaignId = os.path.basename(model_path)
        self.config = config
        self.counter = 0

    def run(self, data_paths):
        self.counter += 1

        p1 = os.fork()
        if p1 != 0:
            os.waitpid(p1, 0)
        else:
            p2 = os.fork()
            if p2 != 0:
                os._exit(0)
            else:
                self.run_fork(data_paths)
                os._exit(0)
    
    def _parse_date(self, d):
        d = d.split('+')[0]
        if '.' in d:
            d = d.split('.')[0]
        
        p = datetime.strptime(d.split('+')[0], '%Y-%m-%dT%H:%M:%S')
        return int(p.strftime('%s')) 
    
    def run_fork(self, data_paths):
        self.clusterer = HashtagClusterer(self.config['clusterer'])
        self.publisher = HashtagPublisher(self.config['publisher'])
        
        text_path, timestamp_path = data_paths
        
        timestamps = open(timestamp_path).read().splitlines()
        timestamps = map(self._parse_date, timestamps)
        
        n_records = len(timestamps)
        time_interval = np.percentile(timestamps, [1, 99])
        
        # Train model
        logging.info("Training: %s (%d records)" % (self.model_path, n_records))
        self.model = self.train(text_path)
        
        if not self.model:
            logging.info("Failed to train: %s" % self.model_path)
            return

        label_vectors = self.get_label_vectors()
        if label_vectors:
            logging.info("Clustering: %s" % self.model_path)
            clusters = self.clusterer(label_vectors)

            logging.info("Publishing: %s" % self.model_path)
            self.publisher(self.campaignId, clusters, time_interval, self.counter, n_records)

            logging.info("Published: %s" % self.model_path)

    def train(self, text_path):
        return ft.supervised(
            text_path,
            self.model_path,
            **self.config['fasttext']
        )
    
    def get_label_vectors(self):
        vecs = [self.model._model.dict_get_label_vector(i) 
            for i in range(self.model._model.dict_nlabels())]
        
        if len(vecs):
            return {
                "labs": np.array(self.model.labels),
                "vecs": np.vstack(vecs),
                "lab_counts": np.array(self.model._model.dict_get_label_counts()),
            }
        else:
            return None

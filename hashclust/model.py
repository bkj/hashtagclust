import os
import logging
import numpy as np
import fasttext as ft

from clusterer import HashtagClusterer
from publisher import HashtagPublisher


class HashtagSupervised:

    def __init__(self, output, config):
        self.output = output
        self.campaignId = os.path.basename(output)

        self.config = config
        self.counter = 0

    def run(self, timestamps, data_path):
        self.counter += 1

        p1 = os.fork()
        if p1 != 0:
            os.waitpid(p1, 0)
        else:
            p2 = os.fork()
            if p2 != 0:
                os._exit(0)
            else:
                self.run_fork(timestamps, data_path)
                os._exit(0)

    def run_fork(self, timestamps, data_path):
        self.clusterer = HashtagClusterer(self.config['clusterer'])
        self.publisher = HashtagPublisher(self.config['publisher'])

        n_records = len(timestamps)
        model_name = '%s-%d' % (self.output, self.counter)

        # Train model
        logging.info("Training: %s (%d records)" % (model_name, n_records))
        self.model = self.train(model_name, data_path)

        if not self.model:
            logging.info("Failed to train: %s" % model_name)
            return

        label_vectors = self.get_label_vectors()
        if label_vectors:
            logging.info("Clustering: %s" % model_name)
            clusters = self.clusterer(label_vectors)

            logging.info("Publishing: %s" % model_name)
            time_interval = [min(timestamps), max(timestamps)]
            self.publisher(self.campaignId, clusters, time_interval, self.counter, n_records)

            logging.info("Published: %s" % model_name)

    def train(self, model_name, data_path):
        try:
            return ft.supervised(
                data_path,
                model_name,
                **self.config['fasttext']
            )
        except:
            return None

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

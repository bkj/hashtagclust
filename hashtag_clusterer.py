import numpy as np
from scipy.cluster import hierarchy

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
        
        n_clusters = max(5, int(len(labs) * self.config['p_cluster']))
        
        return {
            "labs"       : labs,
            "lab_counts" : lab_counts,
            "clusters"   : hierarchy.cut_tree(link, n_clusters=n_clusters).squeeze()
        }


import fasttext as ft
import numpy as np
import pandas as pd
from scipy.cluster import hierarchy

model_name = "549aa41e-e8bd-4b08-b6e5-d2158ecd30c6-7.bin"
model = ft.load_model('./output/%s' % model_name)

vecs = map(model._model.dict_get_label_vector, range(model._model.dict_nlabels()))    
data = { 
    "labs"       : np.array(model.labels), 
    "vecs"       : np.vstack(vecs),
    "lab_counts" : np.array(model._model.dict_get_label_counts()),
}

sel = data['lab_counts'] > 50

labs = data['labs'][sel]
nvecs = data['vecs'][sel]
nvecs /= np.sqrt((nvecs ** 2).sum(axis=1, keepdims=True))
link = hierarchy.linkage(nvecs, method='average', metric='cosine')

labs[0]
labs[nvecs[0].dot(nvecs.T).argsort()[-10:][::-1]]

clusters = hierarchy.cut_tree(link, n_clusters=12).squeeze()
pd.value_counts(clusters)

labs[clusters == 8]


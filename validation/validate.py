import ujson as json
import pandas as pd
from datetime import datetime

inpath = './bkj-hashtag-test-old-20170213.jl'

data = map(json.loads, open(inpath))
df = pd.DataFrame(data)

df.startDate = df.startDate.apply(lambda x: datetime.utcfromtimestamp(x))
df.endDate = df.endDate.apply(lambda x: datetime.utcfromtimestamp(x))

to_remove = ["newsEventIds", "location", "keywords", "urls", "photos", "importanceScore"]

to



"""

Notes

    - The 1M message window is _way_ too big.


???
"""
    clean_tweet.py
    
    Some functions for cleaning tweets
"""

import sys
import re
import json
import codecs
import emoji
from twutils import clean_tweet

for line in sys.stdin:
    obj = json.loads(line)
    print clean_tweet(obj['clean_body'], lang=obj['lang'])
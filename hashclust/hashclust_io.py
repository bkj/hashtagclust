import twutils
try:
    import ultrajson as json
except:
    import json

# --
# Model

def clean_obj(x):
    lang = x['lang']
    if lang != 'en':
        clean_body = twutils.clean_tweet(x['clean_body'])
    else:
        clean_body = twutils.clean_tweet(x['clean_body'], lang=x['lang'])
    
    for campaign_tag in x['campaign_tags']:
        yield {
            'campaignId': campaign_tag['campaignId'],
            'timestamp': x['timestamp'],
            'clean_body': clean_body,
            'lang' : lang,
        }

def clean_gen(gen):
    for x in gen:
        # try:
        for y in clean_obj(json.loads(x)):
            yield y
        # except:
        #     print "unicode error: %s" % json.dumps(x)
import json
from uuid import uuid1
from kafka import KafkaProducer

class HashtagPublisher:
    
    def __init__(self, config):
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=config['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = config['topic']
        
    def __call__(self, campaignId, data, time_interval, counter, total_message_count):
        clusters = np.unique(data['clusters'])
        for cluster in clusters:
            sel = data['clusters'] == cluster
            hashtags = data['labs'][sel]
            
            obj = {
                "uid": str(uuid1()),
                "campaignId": campaignId,
                
                "startDate": time_interval[0],
                "endDate": time_interval[1],
                
                "label": hashtags[0],
                "hashtags": list(hashtags),
                
                "topicMessageCount": data['lab_counts'][sel].sum(),
                "totalMessageCount": total_message_count,
                
                "newsEventIds": None,
                "location": None,
                "keywords": None,
                "urls": None,
                "photos": None,
                "importanceScore": None,
                
                "counter" : counter
            }
            
            logging.info(json.dumps(obj))
            self.producer.send(self.topic, obj)
        
        self.producer.flush()

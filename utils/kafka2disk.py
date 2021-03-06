#!/usr/bin/env python

"""
    kafka2disk.py
    
    Get messages from Kafka queue
    
    !! sudo service avahi-daemon stop
"""

import argparse
import ujson as json
from kafka import KafkaConsumer

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', type=str, default="dev.qcr-io.traptor.norm")
    parser.add_argument('--group-id', type=str, default=None)
    
    parser.add_argument('--earliest', action="store_true")
    parser.add_argument('--max-records', type=float, default=float('Inf'))
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    consumer = KafkaConsumer(
        args.topic,
        group_id=args.group_id,
        bootstrap_servers=['10.105.0.5:9092', '10.105.0.7:9092', '10.105.0.9:9092', '10.105.0.11:9092', '10.105.0.13:9092'],
        auto_offset_reset='latest' if not args.earliest else 'earliest'
    )
    
    counter = 0
    while counter < args.max_records:
        print consumer.next().value
        counter += 1




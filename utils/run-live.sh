#!/bin/bash

#
# Run from Kafks topic
#

./utils/kafka2disk.py --topic prod.qcr-io.normsplosion.message.out --earliest |\
    jq -rc "{
        lang : .doc.lang,
        campaign_tags: .campaign_tags,
        timestamp: .norm.timestamp,
        clean_body: .norm.body
    }" | hashclust ./config.json

#!/bin/bash

cat data/normsplosion/normsplosion.small | gzip -cd | hashclust ./config.json

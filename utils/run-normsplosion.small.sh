#!/bin/bash

cat data/normsplosion/normsplosion.small | gzip -cd | ./__main__.py

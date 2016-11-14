#!/bin/bash

sudo apt-get update
sudo apt-get install -y wget git build-essential libicu-dev \
    default-jdk python-numpy cython python-scipy libicu-dev

wget https://github.com/stedolan/jq/releases/download/jq-1.5/jq-linux64
chmod +x jq-linux64
sudo mv jq-linux64 /usr/bin/jq

# Install fastText
mkdir -p ~/software
cd ~/software
git clone -b hashclust https://github.com/bkj/fastText
cd fastText
sudo ./make.sh

# Install Python wrapper
git clone -b hashclust https://github.com/bkj/fastText.py
cd fastText.py
python setup.py install

# Install hashtagclust
mkdir -p ~/projects
cd ~/projects
git clone https://github.com/bkj/hashtagclust
cd hashtagclust
git checkout package
pip install -r requirements.txt
python setup.py install
#!/bin/bash

sudo apt-get update
sudo apt-get install -y wget git

wget https://repo.continuum.io/archive/Anaconda2-4.2.0-Linux-x86_64.sh
bash Anaconda2-4.2.0-Linux-x86_64.sh -b -p ~/anaconda2
echo 'export PATH=$HOME/anaconda2/bin:$PATH' >> .bashrc
source .bashrc

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


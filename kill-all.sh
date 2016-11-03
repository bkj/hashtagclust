#!/bin/bash

ps aux | grep "python ./hashtagclust" | awk -F ' ' '{print $2}' | xargs -I {} kill -9 {}

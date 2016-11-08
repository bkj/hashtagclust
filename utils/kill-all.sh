#!/bin/bash

ps aux | grep "python ./hashclust" | awk -F ' ' '{print $2}' | xargs -I {} kill -9 {}

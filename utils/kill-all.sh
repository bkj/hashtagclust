#!/bin/bash

ps aux | grep "python ./__main__" | awk -F ' ' '{print $2}' | xargs -I {} kill -9 {}

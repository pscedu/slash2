#!/bin/bash

python run.py -vvv -o tsuite:rootdir=/tmp/tsuite tsuite:logbase=/tmp/tsuite/log --only run:tests --ignore-tests citrus_tests

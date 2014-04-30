#!/bin/bash

rm -r /tmp/tsuite
python2 run.py -vvv -o tsuite:rootdir=/tmp/tsuite tsuite:logbase=/tmp/tsuite/log --only run:tests store:mongo --ignore-tests citrus_tests

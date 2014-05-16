#!/usr/bin/python2
import sys, json

runtime = json.loads(open(sys.argv[1]).read())

print runtime

sys.exit(0)

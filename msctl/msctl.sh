#!/bin/sh
# $Id$

deployment=${0#*msctl-}
msctl -S /var/run/%n.$deployment.%h.sock "$@"

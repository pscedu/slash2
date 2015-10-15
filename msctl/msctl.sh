#!/bin/sh
# $Id$

deployment=${0#msctl.sh-}
msctl -S /var/run/%n.$deployment.%h.sock "$@"

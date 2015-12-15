#!/bin/sh

dep wget

V=4.3
dst=$(pwd)/linux-$V.tarssh

wget https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-$V.tar.xz
tar xf linux-$V.tar.xz
tar c - linux-$V | ssh localhost "tar xfC - $dst"
diff -r linux-$V $dst

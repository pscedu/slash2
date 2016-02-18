#!/bin/sh

[ $1 -eq 0 ] || exit 0

dep wget

V=5.7.11

exclude_time_start
wget -nv http://dev.mysql.com/get/Downloads/MySQL-5.7/mysql-$V.tar.gz
exclude_time_end

tar zxf mysql-$V.tar.gz
cd mysql-$V
./configure
make
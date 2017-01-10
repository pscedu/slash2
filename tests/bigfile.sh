#!/bin/sh
#
# Date: 01/03/2017
#
# Big file tests written for slash2 over the years.
#

if [ $# -eq 1 ]
then
    mypath=$1
else
    mypath=/zzh-slash2/zhihui
fi

START=`date +%s%N`
myhost=$(hostname -s)

cc -o bigfile1 bigfile1.c
cc -o bigfile2 bigfile2.c
cc -o bigfile3 bigfile3.c -lpthread

./bigfile1                                     $mypath/$myhost.bigfile1-1.dat | tee $myhost.bigfile1-1.log

./bigfile2                                     $mypath/$myhost.bigfile2-1.dat | tee $myhost.bigfile2-1.log
./bigfile2 -r                                  $mypath/$myhost.bigfile2-1.dat | tee $myhost.bigfile2-1.log

./bigfile2    -s 4499 -b 12348 -n 3993777      $mypath/$myhost.bigfile2-2.dat | tee $myhost.bigfile2-2.log
./bigfile2 -r -s 4499 -b 7790                  $mypath/$myhost.bigfile2-2.dat | tee $myhost.bigfile2-2.log


./bigfile3                                     $mypath/$myhost.bigfile3-1.dat | tee $myhost.bigfile3-1.log

./bigfile3 -s  7738 -t  6 -b  71785  -n 243656 $mypath/$myhost.bigfile3-2.dat | tee $myhost.bigfile3-2.log

./bigfile3 -s  3873 -t  7 -b  91785  -n 143659 $mypath/$myhost.bigfile3-3.dat | tee $myhost.bigfile3-3.log

./bigfile3 -s  3805 -t  9 -b   1785  -n 443957 $mypath/$myhost.bigfile3-4.dat | tee $myhost.bigfile3-4.log
  
./bigfile3 -s  9805 -t  3 -b 111785  -n 143206 $mypath/$myhost.bigfile3-5.dat | tee $myhost.bigfile3-5.log

./bigfile3 -s  1805 -t  5 -b  81025  -n 213258 $mypath/$myhost.bigfile3-6.dat | tee $myhost.bigfile3-6.log

./bigfile3 -s  1234 -t 11 -b  21029  -n 110052 $mypath/$myhost.bigfile3-7.dat | tee $myhost-bigfile3-7.log

./bigfile3 -s 91234 -t 13 -b  51029  -n 210052 $mypath/$myhost.bigfile3-8.dat | tee $myhost-bigfile3-8.log

./bigfile3 -s 5555  -t 17 -b 114129  -n 280112 $mypath/$myhost.bigfile3-9.dat | tee $myhost-bigfile3-9.log


END=`date +%s%N`
ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
echo "Total time $ELAPSED seconds"

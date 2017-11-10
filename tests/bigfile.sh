#!/bin/sh
#
# Date: 01/03/2017
#
# Big file tests written for slash2 over the years.
#
# Result of 07/02/2017 (orange-yuzu-lime):
#
# All tests have passed successfully! Total time 83275.18377186 seconds
#

function bail {
    set +o pipefail
    END=`date +%s%N`
    ELAPSED=`echo "scale=0; ($END - $START) / 1000000000" | bc`
    echo
    HOURS=$((ELAPSED/60/60))
    MINS=$(((ELAPSED%3600)/60))
    SECS=$((ELAPSED%60))
    echo "Some tests have failed on $myhost. Total elapsed time: $ELAPSED seconds ($HOURS : $MINS : $SECS)."
    exit 0
}

if [ $# -eq 1 ]
then
    mypath=$1
else
    mypath=/zzh-slash2/zhihui
fi


if [ ! -d "$mypath" ]; then
    echo "Working directory $mypath does not exist, bailing.."
    exit 0
fi

pid=$$
START=`date +%s%N`
myhost=$(hostname -s)

cc -o bigfile1 bigfile1.c
cc -o bigfile2 bigfile2.c
cc -o bigfile3 bigfile3.c -lpthread

set -o pipefail

./bigfile1                                     $mypath/$myhost.bigfile1-1.$pid.dat | tee $myhost.bigfile1-1.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile1 -n 20 -s 9901                       $mypath/$myhost.bigfile1-2.$pid.dat | tee $myhost.bigfile1-2.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile2                                     $mypath/$myhost.bigfile2-1.$pid.dat | tee $myhost.bigfile2-1.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile2 -r -d                               $mypath/$myhost.bigfile2-1.$pid.dat | tee $myhost.bigfile2-1.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile2    -s 4499 -b 12348 -n 3993777      $mypath/$myhost.bigfile2-2.$pid.dat | tee $myhost.bigfile2-2.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile2 -r -d -s 4499 -b 7790               $mypath/$myhost.bigfile2-2.$pid.dat | tee $myhost.bigfile2-2.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile3                                     $mypath/$myhost.bigfile3-1.$pid.dat | tee $myhost.bigfile3-1.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile3 -s  7738 -t  6 -b  71785  -n 243656 $mypath/$myhost.bigfile3-2.$pid.dat | tee $myhost.bigfile3-2.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile3 -s  3873 -t  7 -b  91785  -n 143659 $mypath/$myhost.bigfile3-3.$pid.dat | tee $myhost.bigfile3-3.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile3 -s  3805 -t  9 -b   1785  -n 413957 $mypath/$myhost.bigfile3-4.$pid.dat | tee $myhost.bigfile3-4.$pid.log
if [ $? -eq 1 ]
then
    bail
fi
  
./bigfile3 -s  9805 -t  3 -b 111785  -n 133296 $mypath/$myhost.bigfile3-5.$pid.dat | tee $myhost.bigfile3-5.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile3 -s  1805 -t  5 -b  81025  -n 213258 $mypath/$myhost.bigfile3-6.$pid.dat | tee $myhost.bigfile3-6.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile3 -s  1234 -t 25 -b  21029  -n  10052 $mypath/$myhost.bigfile3-7.$pid.dat | tee $myhost.bigfile3-7.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile3 -s 91234 -t 13 -b  51029  -n 207052 $mypath/$myhost.bigfile3-8.$pid.dat | tee $myhost.bigfile3-8.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

./bigfile3 -s  5555 -t 17 -b 114129  -n  77112 $mypath/$myhost.bigfile3-9.$pid.dat | tee $myhost.bigfile3-9.$pid.log
if [ $? -eq 1 ]
then
    bail
fi

set +o pipefail

END=`date +%s%N`
ELAPSED=`echo "scale=0; ($END - $START) / 1000000000" | bc`
echo
HOURS=$((ELAPSED/60/60))
MINS=$(((ELAPSED%3600)/60))
SECS=$((ELAPSED%60))
echo "All tests have passed successfully on $myhost! Total elapsed time: $ELAPSED seconds ($HOURS : $MINS : $SECS)."


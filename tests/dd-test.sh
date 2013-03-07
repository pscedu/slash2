#!/bin/sh
#
# Date: 02/21/2013
#
# Usage: ./dd-test.sh 20 2000 foo 10
#
if [ $# -ne 3 ] && [ $# -ne 4 ]
then
    echo "Usage: dd-test.sh dirs files/dir test-dir [sleep]"
    exit 0
fi

sleeptime=0
if [ $# -eq 4 ]
then
    #
    # Use -ne so that the shell will check if this is an integer 
    #
    # Note that this check will reject hex number, which sleep
    # can actually accept.
    #
    if [ $4 -ne 0 2> /dev/null ]
    then
        sleeptime=$4
    else
       echo "Please enter a decimal integer for the sleep time in seconds"
       exit 0
    fi
fi

if [ $3 = '.' ] || [ $3 = '..' ]
then
    echo "Don't use special dot and dotdot directories"
    exit 0
fi
echo "Sleep $sleeptime seconds between each directories..."

rm -rf $3
START=`date +%s%N`
for i in `seq 1 $1`; do
    mkdir -p $3/$i
    echo $i
    for j in `seq 1 $2`; do
        dd if=/dev/zero of=$3/$i/$j bs=1024 count=1024 > /dev/null 2>&1 &
    done
    sleep $sleeptime 
done
END=`date +%s%N`
ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
echo "Total time $ELAPSED"

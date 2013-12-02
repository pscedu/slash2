#!/bin/bash
#
# 03/11/2013: Build slash2 source code in a loop.
#

function cleanup()
{
    echo ""

    end_time=$SECONDS

    ss=`expr $end_time - $start_time`
    
    mm=`expr $ss / 60` 
    ss=`expr $ss % 60` 
    hh=`expr $mm / 60` 
    mm=`expr $mm % 60` 

    printf "\nTotal time elapsed %02d::%02d::%02d\n" $hh $mm $ss 
    exit 0
}

if [ $# -ne 3 ] 
then
    echo "Usage: build-stress.sh change dir count"
    exit 0
fi

# trap Control+C

trap 'cleanup' 2

start_time=$SECONDS


mydir=`pwd`
for ((i=0; i < $3; i++))
do
    dir=$2.$i

    mkdir $dir
    if [ $? -ne 0 ]
    then
        exit 0
    fi

    svn co -r $1 svn+ssh://frodo/cluster/svn/projects $dir
    if [ $? -ne 0 ]
    then
        exit 0
    fi

    cd $dir
    cd zfs

    make
    if [ $? -ne 0 ]
    then
        exit 0
    fi

    cd ..

    make
    if [ $? -ne 0 ]
    then
        exit 0
    fi

    cd $mydir

done

cleanup

#!/bin/bash 
#
# 12/10/2010: link and unlink files.
#
if [ $# -ne 3 ] 
then
    echo "Usage: ./unlink.sh basename suffix count"
    exit 0
fi

for ((i=0; i < $3; i++))
do
        filename=$1-$i.$2
        if [ -e $filename ]
        then
                echo "$filename exist"
                exit 0
        fi  
        touch $filename
        if [ $? -ne 0 ] 
        then
                echo "fail to create file $filename"
                exit 0
        fi
	id=`stat -c "%i" $filename`
	id=`echo "obase=16; $id" | bc`
	echo "The ID of $filename is $id"
	unlink $filename
done


#!/bin/bash 
#
# 12/10/2010: link and unlink files.
#
if [ $# -ne 3 ] 
then
    echo "Usage: ./unlink.sh basename suffix count"
    exit 0
fi

start_time=$SECONDS

for ((i=0; i < $3; i++))
do
        filename=$1-$i.$2
        if [ -e $filename ]
        then
                echo "$filename already exist, bail."
                exit 0
        fi  

        touch $filename
        if [ $? -ne 0 ] 
        then
                echo "Fail to create file $filename"
                exit 0
        fi

        date > $filename
        if [ $? -ne 0 ] 
        then
                echo "Fail to write file $filename"
                exit 0
        fi
	
	id=`stat -c "%i" $filename`
	id=`echo "obase=10; $id" | bc`
        echo "File $filename $id has been created ..."
done

for ((i=0; i < $3; i++))
do
        filename=$1-$i.$2
	unlink $filename
        if [ $? -ne 0 ] 
        then
                echo "Fail to unlink file $filename"
                exit 0
        fi
        echo "File $filename has been unlinked ..."
done

end_time=$SECONDS
echo
printf "Total file created and then unlinked: %ld.\n" $3 
printf "Total elapsed time: %ld seconds.\n" $total $(($end_time - $start_time))

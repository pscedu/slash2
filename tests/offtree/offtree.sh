#!/bin/bash

iters=50
ppslb=32
nslbs=128
mapsz=128

o=0; 
while [ $o -lt 127 ]
do
  x=1;
  while [ $x -lt 128 ] 
    do
    if [ $x -lt $o ]; then let x=$o; fi    
    PSC_LOG_LEVEL=1 ./offtree_test -i ${iters} -B ${x} -n ${ppslb} -d ${nslbs} -m ${mapsz} -I 1 -O ${o} 2> /dev/null
    echo "-- RC=$? -i ${iters} -B ${x} -n ${ppslb} -d ${nslbs} -m ${mapsz} -I 1 -O ${o} ----"; 
    let x=$x+1; 
  done
  let o=$o+1; 
done

x=1; 
while [ $x -lt 128 ] ; 
  do 
  PSC_LOG_LEVEL=1 ./offtree_test -i ${iters} -B ${x} -n ${ppslb} -d ${nslbs} -m ${mapsz} -I 0  2> /dev/null
  echo "-- RC=$? -i ${iters} -B ${x} -n ${ppslb} -d ${nslbs} -m ${mapsz} -I 0  ----"; 
  let x=$x+1; 
done




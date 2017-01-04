#!/bin/sh
#
# Date: 01/03/2017
#
#
START=`date +%s%N`

cc bigfile3.c -lpthread

./a.out /zzh-slash2/zhihui/bigfile1.dat | tee bigfile1.log

./a.out -s 7738 -t 6 -b 71785   -n 243656 /zzh-slash2/zhihui/bigfile2.dat | tee bigfile2.log

./a.out -s 3873 -t 7 -b 91785   -n 143659 /zzh-slash2/zhihui/bigfile3.dat | tee bigfile3.log

./a.out -s 3805 -t 9 -b 1785    -n 443957 /zzh-slash2/zhihui/bigfile4.dat | tee bigfile4.log
 
./a.out -s 9805 -t 3 -b 111785  -n 143206 /zzh-slash2/zhihui/bigfile5.dat | tee bigfile5.log

./a.out -s 1805 -t 5 -b 81025   -n 213258 /zzh-slash2/zhihui/bigfile6.dat | tee bigfile6.log

./a.out /zzh-slash2/zhihui/bigfile7.dat | tee bigfile7.log

END=`date +%s%N`
ELAPSED=`echo "scale=8; ($END - $START) / 1000000000" | bc`
echo "Total time $ELAPSED seconds"

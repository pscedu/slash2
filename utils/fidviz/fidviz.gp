set term png small color picsize 1024 768
set title "disk I/O performance of outer and inner 10% of disk\nfor writes on $disk, blocksize $blksz"
set xlabel "time (seconds)"
set ylabel "bandwidth observed (MB/sec)"
set output "img.png"
set grid
#set yr [0:20000]

plot	"data" using 1:2 title "data points", \
	"data" using 1:2 smooth bezier title "bezier" with lines

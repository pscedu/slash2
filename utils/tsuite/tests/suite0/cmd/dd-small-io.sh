dd if=$RANDOM bs=512 of=outfile
diff -q $RANDOM outfile

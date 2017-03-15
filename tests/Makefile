# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

SUBDIRS+=	config
SUBDIRS+=	replbit

include ${SLASHMK}


bigfile: bigfile1 bigfile2 bigfile3

bigfile1: bigfile1.c
	gcc -o bigfile1 bigfile1.c

bigfile2: bigfile2.c
	gcc -o bigfile2 bigfile2.c

bigfile3: bigfile3.c
	gcc -o bigfile3 bigfile3.c -lpthread

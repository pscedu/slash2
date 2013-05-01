# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

PROG=		slauthd
BIN=		slauthd.sh
MAN+=		slauthd.8
SRCS+=		main_authd.c
#SRCS+=		${SLASH_BASE}/share/cfg_common.c
#SRCS+=		${SLASH_BASE}/share/slutil.c
SRCS+=		${PFL_BASE}/pthrutil.c
SRCS+=		${PFL_BASE}/random.c
SRCS+=		${PFL_BASE}/rlimit.c
SRCS+=		${PFL_BASE}/stree.c
SRCS+=		${PFL_BASE}/thread.c
SRCS+=		${PFL_BASE}/vbitmap.c
SRCS+=		${PFL_BASE}/waitq.c

MODULES+=	pthread lnet gcrypt rpc ctl

include ${SLASHMK}

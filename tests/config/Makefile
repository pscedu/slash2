# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

TEST=		slconfig_test
SRCS+=		slconfig_test.c
SRCS+=		${SLASH_BASE}/share/cfg_common.c
SRCS+=		${SLASH_BASE}/share/lconf.l
SRCS+=		${SLASH_BASE}/share/slerr.c
SRCS+=		${SLASH_BASE}/share/yconf.y

MODULES+=	lnet pthread rpc clock pfl
DEFINES+=	-D_SLASH_CLIENT

include ${SLASHMK}

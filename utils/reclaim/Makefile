# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		reclaim
MAN=		reclaim.8
SRCS+=		reclaim.c
MODULES+=	pfl

include ${SLASHMK}

ifneq ($(filter mds,${SLASH_MODULES}),)
  FORCE_INST=	1
endif

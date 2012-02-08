# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		reclaim
SRCS+=		reclaim.c
MODULES+=	pfl

include ${SLASHMK}

ifneq ($(filter mds,${SLASH_MODULES}),)
  FORCE_INST=	1
endif

# $Id: Makefile 18220 2011-10-21 18:59:27Z yanovich $

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		reclaim
SRCS+=		reclaim.c
MODULES+=	pfl

include ${SLASHMK}

ifneq ($(filter mds,${SLASH_MODULES}),)
  FORCE_INST=	1
endif

# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		cursor_mgr
SRCS+=		cursor_mgr.c
MODULES+=	pfl

include ${SLASHMK}

ifneq ($(filter mds,${SLASH_MODULES}),)
  FORCE_INST=	1
endif

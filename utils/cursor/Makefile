# $Id$

ROOTDIR=../../..
include ${ROOTDIR}/Makefile.path

PROG=		cursor_mgr
SRCS+=		cursor_mgr.c
MODULES+=	pfl

ifneq ($(filter mds,${SLASH_MODULES}),)
  FORCE_INST=	1
endif

include ${SLASHMK}

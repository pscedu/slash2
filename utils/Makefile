# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

SUBDIRS+=	prslerr
SUBDIRS+=	typedump

include ${SLASHMK}

ifneq ($(filter ${SLASH_MODULES},mds),)
  SUBDIRS+=	cursor
  SUBDIRS+=	dumpfid
  SUBDIRS+=	reclaim
endif

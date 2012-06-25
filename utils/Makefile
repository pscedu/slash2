# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path

SUBDIRS+=	prslerr
SUBDIRS+=	typedump

ifneq ($(filter ${SLASH_MODULES},mds),)
  SUBDIRS+=	cursor
  SUBDIRS+=	dumpfid
  SUBDIRS+=	reclaim
endif

include ${SLASHMK}

# $Id$

ROOTDIR=../..
include ${ROOTDIR}/Makefile.path
include ${SLASH_BASE}/mk/pre.mk

SUBDIRS+=	prslerr
SUBDIRS+=	typedump

ifneq ($(filter ${SLASH_MODULES},zfs),)
  SUBDIRS+=	cursor
  SUBDIRS+=	dumpfid
  SUBDIRS+=	reclaim
endif

include ${SLASHMK}

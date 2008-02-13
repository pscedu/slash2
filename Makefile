# $Id$

SLASH_BASE=.
PROJECT_BASE=${SLASH_BASE}
include Makefile.path
include ${SLASHMK}

SUBDIRS+=	mount_slash
SUBDIRS+=	newfs_slio
SUBDIRS+=	newfs_slmd
SUBDIRS+=	slashd
SUBDIRS+=	slctl
SUBDIRS+=	slioctl
SUBDIRS+=	sliod

build:
	${MAKE} clean depend all

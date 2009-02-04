# $Id$

SLASH_BASE=.
PROJECT_BASE=${SLASH_BASE}
include Makefile.path
include ${SLASHMK}

SUBDIRS+=	mount_slash
SUBDIRS+=	msctl
SUBDIRS+=	slashd
SUBDIRS+=	slctl
SUBDIRS+=	slioctl
SUBDIRS+=	sliod

build:
	@(cd ${ROOTDIR}/zfs/zfs-fuse-0.5.0_slash/src && \
	  scons slashlib=1)
	${MAKE} clean && ${MAKE} depend && ${MAKE} all

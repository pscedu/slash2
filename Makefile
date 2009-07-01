# $Id$

SLASH_BASE=.
PROJECT_BASE=${SLASH_BASE}
include Makefile.path
include ${SLASHMK}

SUBDIRS+=	mount_slash
SUBDIRS+=	msctl
SUBDIRS+=	slashd
SUBDIRS+=	slctl
SUBDIRS+=	slimmns
SUBDIRS+=	slioctl
SUBDIRS+=	sliod
SUBDIRS+=	slmkjrnl

zbuild:
	@(cd ${ZFS_BASE} && ${SCONS} -c && scons)
	@(cd ${ZFS_BASE} && ${SCONS} slashlib=1 -c && ${SCONS} slashlib=1)

build:
	@(cd ${ZFS_BASE} && ${SCONS} slashlib=1)
	${MAKE} clean && ${MAKE} depend && ${MAKE} all

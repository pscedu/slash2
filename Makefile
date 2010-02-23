# $Id$

SLASH_BASE=.
PROJECT_BASE=${SLASH_BASE}
include Makefile.path
include ${SLASHMK}

SUBDIRS+=	mount_slash
SUBDIRS+=	msctl
SUBDIRS+=	slashd
SUBDIRS+=	slictl
SUBDIRS+=	slimmns
SUBDIRS+=	sliod
SUBDIRS+=	slkeymgt
SUBDIRS+=	slmctl
SUBDIRS+=	slmkjrnl

MAN+=		doc/sladm.7
MAN+=		doc/slash.conf.5

zbuild:
	@(cd ${ZFS_BASE} && ${SCONS} ${ZFS_SCONSOPTS} -c &&		\
	    ${SCONS} ${ZFS_SCONSOPTS})
	@(cd ${ZFS_BASE} &&						\
	    ${SCONS} slashlib=1 debug=4 ${ZFS_SCONSOPTS} -c &&		\
	    ${SCONS} slashlib=1 debug=4 ${ZFS_SCONSOPTS})

build-prereq rezbuild:
	@(cd ${ZFS_BASE} && ${SCONS} slashlib=1 debug=4 ${ZFS_SCONSOPTS})

fullbuild: zbuild build

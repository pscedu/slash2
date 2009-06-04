# $Id$

SCONS?=		scons

CFLAGS+=	-Wall -W -g
#CFLAGS+=	-Wshadow -Wunused -Wuninitialized
DEFINES+=	-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DYY_NO_UNPUT
INCLUDES+=	-I${KERNEL_BASE}/include

THREAD_LIBS?=	-lpthread

FUSE_INCLUDES=	`${PKG_CONFIG} --cflags fuse | perl -ne 'print $$& while /-I\S+\s?/gc'`
FUSE_DEFINES=	`${PKG_CONFIG} --cflags fuse | perl -ne 'print $$& while /-D\S+\s?/gc'`
FUSE_CFLAGS=	`${PKG_CONFIG} --cflags fuse | perl -ne 'print $$& while /-[^ID]\S+\s?/gc'`
FUSE_LIBS=	`${PKG_CONFIG} --libs fuse`

LIBL?=		-ll

ZFS_LIBS=	-L${ZFS_BASE}/zfs-fuse				\
		-L${ZFS_BASE}/lib/libavl			\
		-L${ZFS_BASE}/lib/libnvpair			\
		-L${ZFS_BASE}/lib/libsolkerncompat		\
		-L${ZFS_BASE}/lib/libumem			\
		-L${ZFS_BASE}/lib/libzfscommon			\
		-L${ZFS_BASE}/lib/libzpool			\
		-lzfs-fuse -lzpool-kernel -lzfscommon-kernel	\
		-lnvpair-kernel -lavl -lumem -lsolkerncompat

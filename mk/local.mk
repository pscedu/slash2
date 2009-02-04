# $Id$

CFLAGS+=	-Wall -W -g
#CFLAGS+=	-Wshadow -Wunused -Wuninitialized
DEFINES+=	-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DYY_NO_UNPUT
INCLUDES+=	-I${KERNEL_BASE}/include

THREAD_LIBS?=	-lpthread
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

# On altix
ifneq ($(wildcard /opt/sgi),)
#DEFINES+=      -DCONFIG_NR_CPUS=2 -D_GNU_SOURCE
DEFINES+=      -DCONFIG_NR_CPUS=2
endif

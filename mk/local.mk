# $Id$

INCLUDES+=	-I${KERNEL_BASE}/include

ZFS_LIBS=	-L${ZFS_BASE}/zfs-fuse				\
		-L${ZFS_BASE}/lib/libavl			\
		-L${ZFS_BASE}/lib/libnvpair			\
		-L${ZFS_BASE}/lib/libsolkerncompat		\
		-L${ZFS_BASE}/lib/libumem			\
		-L${ZFS_BASE}/lib/libzfscommon			\
		-L${ZFS_BASE}/lib/libzpool			\
		-lzfs-fuse -lzpool-kernel -lzfscommon-kernel	\
		-lnvpair-kernel -lavl -lumem -lsolkerncompat

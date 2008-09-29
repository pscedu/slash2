# $Id$

DEFINES+=	-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64 -DLINUX
INCLUDES+=	-I${KERNEL_BASE}/include

THREAD_LIBS?=	-lpthread
LIBL?=		-ll

# On altix
ifneq ($(wildcard /opt/sgi),)
#DEFINES+=      -DCONFIG_NR_CPUS=2 -D_GNU_SOURCE
DEFINES+=      -DCONFIG_NR_CPUS=2
PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
endif

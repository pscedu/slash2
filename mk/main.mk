# $Id$

include ${SLASH_BASE}/mk/local.mk

INCLUDES+=	-I${PFL_BASE}/include
INCLUDES+=	-I${SLASH_BASE}/include
INCLUDES+=	-I${SLASH_BASE} -I.
INCLUDES+=	-I${ZFS_BASE}
INCLUDES+=	-I${LNET_BASE}/include

include ${MAINMK}

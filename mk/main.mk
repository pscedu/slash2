# $Id$

include ${SLASH_BASE}/mk/local.mk

INCLUDES+=	-I${SLASH_BASE}/include
INCLUDES+=	-I${SLASH_BASE}

DEFINES+=	-DAPP_STRERROR=slstrerror

SRC_PATH+=	${ZFS_BASE} ${SLASH_BASE}

include ${MAINMK}

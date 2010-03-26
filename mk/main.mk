# $Id$

include ${SLASH_BASE}/mk/local.mk

INCLUDES+=	-I${SLASH_BASE}/include
INCLUDES+=	-I${SLASH_BASE}

DEFINES+=	-DAPP_STRERROR=slstrerror

SRC_PATH+=	${ZFS_BASE}
ifneq ($(realpath ${SLASH_BASE}),${CURDIR})
SRC_PATH+=	${SLASH_BASE}
endif

include ${MAINMK}

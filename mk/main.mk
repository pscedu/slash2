# $Id$

INCLUDES+=		-I${SLASH_BASE}/include
INCLUDES+=		-I${SLASH_BASE}

DEFINES+=		-DAPP_STRERROR=slstrerror

SRC_PATH+=		${ZFS_BASE}
ifneq ($(realpath ${SLASH_BASE}),${CURDIR})
SRC_PATH+=		${SLASH_BASE}
endif

SLASH_MODULES+=		cli
SLASH_MODULES+=		ion
SLASH_MODULES+=		mds

-include ${SLASH_BASE}/mk/local.mk

include ${MAINMK}

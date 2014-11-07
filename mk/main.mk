# $Id$

INCLUDES+=		-I${SLASH_BASE}/include
INCLUDES+=		-I${SLASH_BASE}
INCLUDES+=		${SQLITE3_INCLUDES}

DEFINES+=		-DSL_STK_VERSION=$$(svn info | awk '{ if ($$0 ~ /^Revision: /) print $$2 }')
SRCS+=			${SLASH_BASE}/share/slerr.c

SRC_PATH+=		${SLASH_BASE}/include
SRC_PATH+=		${SLASH_BASE}/mount_slash
SRC_PATH+=		${SLASH_BASE}/share
SRC_PATH+=		${SLASH_BASE}/slashd
SRC_PATH+=		${SLASH_BASE}/sliod

SLASH_MODULES?=		cli ion mds

-include ${SLASH_BASE}/mk/local.mk

ifeq (${CURDIR},$(realpath ${SLASH_BASE}/mount_slash))
 ifneq ($(filter acl,${SLASH_OPTIONS}),)
  SRCS+=		${SLASH_BASE}/mount_slash/acl_cli.c
  DEFINES+=		-DSLOPT_POSIX_ACLS
  DEFINES+=		-DSLOPT_POSIX_ACLS_REVERT
  LDFLAGS+=		-lacl
 endif
endif

include ${MAINMK}

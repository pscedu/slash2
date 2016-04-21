# $Id$
# %GPL_START_LICENSE%
# ---------------------------------------------------------------------
# Copyright 2015-2016, Google, Inc.
# Copyright (c) 2007-2015, Pittsburgh Supercomputing Center (PSC).
# All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at
# your option) any later version.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License contained in the file
# `COPYING-GPL' at the top of this distribution or at
# https://www.gnu.org/licenses/gpl-2.0.html for more details.
# ---------------------------------------------------------------------
# %END_LICENSE%

include ${SLASH_BASE}/mk/pre.mk
include ${MAINMK}

$(call ADD_FILE_CFLAGS,${SLASH_BASE}/share/adler32.c,${COPT})

${OBJDIR}/version.o: $(filter-out ${SLASH_BASE}/share/version.c},${SRCS})

${OBJDIR}/rpc_names.c: ${SLASH_BASE}/include/slashrpc.h
	${MAKE} ${OBJDIR}
	{								\
		echo '#include <stdlib.h>';				\
		echo;							\
		echo 'const char *slrpc_names[] = { NULL,';		\
		perl -Wne 'print lc qq{\t"$$1",\n} if /SRMT_([A-Z_]+)/'	\
		    ${SLASH_BASE}/include/slashrpc.h;			\
		echo '};';						\
	} > $@

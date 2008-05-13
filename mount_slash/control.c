/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running instance of mount_slash.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/uio.h>

#include <err.h>
#include <errno.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "psc_ds/hash.h"
#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_util/threadtable.h"
#include "psc_util/thread.h"
#include "psc_util/cdefs.h"

#include "control.h"
#include "mount_slash.h"


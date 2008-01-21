/* $Id: control.c 2444 2008-01-08 17:49:30Z yanovich $ */

/*
 * Control interface for querying and modifying
 * parameters of a currently-running zestiond.
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

#include "zestHash.h"
#include "zestInode.h"
#include "zestChunkMap.h"
#include "zestInodeCache.h"
#include "zestIoThread.h"
#include "zestList.h"
#include "zestMList.h"
#include "zestParityGroupCache.h"
#include "zestParityThr.h"
#include "zestRead.h"
#include "zestThreadTable.h"

#include "ciod.h"
#include "control.h"
#include "disk.h"
#include "vbitmap.h"
#include "zestion.h"
#include "cdefs.h"

struct psc_thread zestionControlThread;

#define Q 15	/* listen() queue */

/*
 * zctlthr_sendmsgv - send a control message back to client.
 * @fd: client socket descriptor.
 * @zmch: already filled-out zestion control message header.
 * @zcm: zestion control message contents.
 */
void
zctlthr_sendmsgv(int fd, const struct zctlmsghdr *zcmh, const void *zcm)
{
	struct iovec iov[2];
	size_t tsiz;
	ssize_t n;

	iov[0].iov_base = (void *)zcmh;
	iov[0].iov_len = sizeof(*zcmh);

	iov[1].iov_base = (void *)zcm;
	iov[1].iov_len = zcmh->zcmh_size;

	n = writev(fd, iov, NENTRIES(iov));
	if (n == -1)
		err(1, "write");
	tsiz = sizeof(*zcmh) + zcmh->zcmh_size;
	if ((size_t)n != tsiz)
		warn("short write");
	zctlthr(&zestionControlThread)->zc_st_nsent++;
	sched_yield();
}

/*
 * zctlthr_sendmsg - send a control message back to client.
 * @fd: client socket descriptor.
 * @type: type of message.
 * @siz: size of message.
 * @zcm: zestion control message contents.
 * Notes: a zestion control message header will be constructed and
 * written to the client preceding the message contents.
 */
void
zctlthr_sendmsg(int fd, int type, size_t siz, const void *zcm)
{
	struct iovec iov[2];
	struct zctlmsghdr zcmh;
	size_t tsiz;
	ssize_t n;

	memset(&zcmh, 0, sizeof(zcmh));
	zcmh.zcmh_type = type;
	zcmh.zcmh_size = siz;

	iov[0].iov_base = &zcmh;
	iov[0].iov_len = sizeof(zcmh);

	iov[1].iov_base = (void *)zcm;
	iov[1].iov_len = siz;

	n = writev(fd, iov, NENTRIES(iov));
	if (n == -1)
		err(1, "write");
	tsiz = sizeof(zcmh) + siz;
	if ((size_t)n != tsiz)
		warn("short write");
	zctlthr(&zestionControlThread)->zc_st_nsent++;
	sched_yield();
}

/*
 * zctlthr_senderrmsg - send an error message to client.
 * @fd: client socket descriptor.
 * @fmt: printf(3) format of error message.
 */
void
zctlthr_senderrmsg(int fd, struct zctlmsghdr *zcmh, const char *fmt, ...)
{
	struct zctlmsg_errmsg zem;
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(zem.zem_errmsg, sizeof(zem.zem_errmsg), fmt, ap);
	va_end(ap);

	zcmh->zcmh_type = ZCMT_ERRMSG;
	zcmh->zcmh_size = sizeof(zem);
	zctlthr_sendmsgv(fd, zcmh, &zem);
}

/*
 * zctlthr_sendrep_getstats - send a response to a "getstats" inquiry.
 * @fd: client socket descriptor.
 * @zcmh: already filled-in zestion control message header.
 * @zs: thread stats message structure to be filled in and sent out.
 * @zthr: zestion thread begin queried.
 * @probe: whether to send empty msgs for threads which do not track stats.
 */
void
zctlthr_sendrep_getstats(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_stats *zst, struct psc_thread *zthr, int probe)
{
	snprintf(zst->zst_threadname, sizeof(zst->zst_threadname),
	    "%s", zthr->pscthr_name);
	zst->zst_threadtype = zthr->pscthr_type;
	switch (zthr->pscthr_type) {
	case ZTHRT_CTL:
		zst->zst_nclients = zctlthr(zthr)->zc_st_nclients;
		zst->zst_nsent    = zctlthr(zthr)->zc_st_nsent;
		zst->zst_nrecv    = zctlthr(zthr)->zc_st_nrecv;
		break;
	case ZTHRT_RPCMDS:
		zst->zst_nopen  = zrpcmdsthr(zthr)->zrm_st_nopen;
		zst->zst_nclose = zrpcmdsthr(zthr)->zrm_st_nclose;
		zst->zst_nstat  = zrpcmdsthr(zthr)->zrm_st_nstat;
		break;
	case ZTHRT_RPCIO:
		zst->zst_nwrite = zrpciothr(zthr)->zri_st_nwrite;
		break;
	default:
		if (probe)
			return;
		break;
	}
	zctlthr_sendmsgv(fd, zcmh, zst);
}

/*
 * zctlthr_sendreps_getmlist - respond to a "getmlist" inquiry.
 * @fd: client socket descriptor.
 * @zcmh: already filled-in zestion control message header.
 * @zm: mlist message structure to be filled in and sent out.
 */
void
zctlthr_sendreps_getmlist(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_mlist *zm)
{
	char name[ZML_NAME_MAX];
	struct zest_mlist *zml;
	int found, all;

	found = 0;
	snprintf(name, sizeof(name), "%s", zm->zm_name);
	all = (strcmp(name, ZML_NAME_ALL) == 0);

	spinlock(&zestMultiListsLock);
	psclist_for_each_entry(zml, &zestMultiLists, zml_index_lentry)
		if (all || strncmp(zml->zml_name, name,
		    strlen(name)) == 0) {
			found = 1;

			snprintf(zm->zm_name, sizeof(zm->zm_name),
			    "%s", zml->zml_name);
			zm->zm_size = zml->zml_size;
			zm->zm_nseen = zml->zml_nseen;
			zm->zm_waitors =
			    multilock_cond_nwaitors(&zml->zml_mlockcond_empty);
			zctlthr_sendmsgv(fd, zcmh, zm);

			/*
			 * Exact matches should terminate
			 * further searching.
			 */
			if (strlen(name) == strlen(zml->zml_name))
				break;
		}
	freelock(&zestMultiListsLock);
	if (!found && !all)
		zctlthr_senderrmsg(fd, zcmh,
		    "unknown mlist: %s", name);
}

/*
 * zctlthr_sendreps_getmeter - respond to a "getmeter" inquiry.
 * @fd: client socket descriptor.
 * @zcmh: already filled-in zestion control message header.
 * @zmtr: meter message structure to be filled in and sent out.
 */
void
zctlthr_sendreps_getmeter(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_meter *zmtr)
{
	char name[ZMETER_NAME_MAX];
	struct zmeter *mi;
	int found, all;

	found = 0;
	snprintf(name, sizeof(name), "%s", zmtr->zmtr_mtr.zmtr_name);
	all = (strcmp(name, ZMTR_NAME_ALL) == 0);

	spinlock(&zmetersLock);
	psclist_for_each_entry(mi, &zmetersList, zmtr_lentry)
		if (all || strncmp(mi->zmtr_name, name,
		    strlen(name)) == 0) {
			found = 1;

			zmtr->zmtr_mtr = *mi;
			zctlthr_sendmsgv(fd, zcmh, zmtr);

			/* Terminate on exact match. */
			if (strlen(name) == strlen(mi->zmtr_name))
				break;
		}
	freelock(&zmetersLock);
	if (!found && !all)
		zctlthr_senderrmsg(fd, zcmh, "unknown meter: %s", name);
}

/*
 * zctlthr_sendrep_getloglevel - send a response to a "getloglevel" inquiry.
 * @fd: client socket descriptor.
 * @zcmh: already filled-in zestion control message header.
 * @zll: loglevel message structure to be filled in and sent out.
 * @zthr: zestion thread begin queried.
 */
void
zctlthr_sendrep_getloglevel(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_loglevel *zll, struct psc_thread *zthr)
{
	snprintf(zll->zll_threadname, sizeof(zll->zll_threadname),
	    "%s", zthr->pscthr_name);
	memcpy(zll->zll_levels, zthr->pscthr_loglevels,
	    sizeof(zthr->pscthr_loglevels));
	zctlthr_sendmsgv(fd, zcmh, zll);
}

/*
 * zctlthr_sendrep_getdisk - send a response to a "getdisk" inquiry.
 * @fd: client socket descriptor.
 * @zcmh: already filled-in zestion control message header.
 * @zthr: zestion I/O thread begin queried.
 */
void
zctlthr_sendrep_getdisk(int fd, struct zctlmsghdr *zcmh,
    struct psc_thread *zthr)
{
	struct zctlmsg_disk *zd;
	struct zestion_disk *disk;
	struct vbitmap *vb;
	int errno_save;
	size_t tblsiz;

	vb = NULL; /* gcc */
	tblsiz = 0;
	disk = ziothr(zthr)->zi_disk;
	if (!disk->zd_failed) {
		vb = disk->zd_usedtab;
		tblsiz = vb->vb_end - vb->vb_start;
	}
	zcmh->zcmh_size = sizeof(*zd) + tblsiz;
	if ((zd = malloc(zcmh->zcmh_size)) == NULL) {
		errno_save = errno;
		psc_error("malloc");
		zctlthr_senderrmsg(fd, zcmh, "%s",
		    strerror(errno_save));
		return;
	}
	memset(zd, 0, zcmh->zcmh_size);
	snprintf(zd->zd_threadname, sizeof(zd->zd_threadname),
	    "%s", zthr->pscthr_name);
	snprintf(zd->zd_diskdevice, sizeof(zd->zd_diskdevice),
	    "%s", disk->zd_device ? disk->zd_device : "N/A");
	if (!disk->zd_failed)
		memcpy(zd->zd_usedtab, vb->vb_start, tblsiz);
	zctlthr_sendmsgv(fd, zcmh, zd);
	free(zd);
}

/*
 * zctlthr_sendrep_getinode - send a response to a "getinode" inquiry.
 * @fd: client socket descriptor.
 * @zcmh: already filled-in zestion control message header.
 * @zinom: inode message structure to be filled in and sent out.
 */
void
zctlthr_sendrep_getinode(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_inode *zinom)
{
	struct hash_entry_str *he;
	struct hash_bucket *hb;
	struct psclist_head *ent;
	zinode_t *ino;
	int n;

	for (n = 0; n < zinodeHashTable.htable_size; n++) {
		hb = &zinodeHashTable.htable_buckets[n];
		LOCK_BUCKET(hb);
		psclist_for_each(ent, &hb->hbucket_list) {
			he = psclist_entry(ent, struct hash_entry_str,
			    hentry_str_list);
			ino = he->private;

			/* XXX do not lock */
			ZINODE_LOCK(ino);
			COPYFID(&zinom->zi_fid, &ino->zinode_fid);
			zinom->zi_state = ino->zinode_state;
			zinom->zi_nptygrps = ino->zinode_nptygrps;
			zinom->zi_opencount =
			    atomic_read(&ino->zinode_opencount);
			zinom->zi_ndirty_chunks =
			    atomic_read(&ino->zinode_ndirty_chunks);
			zinom->zi_guid = ino->zinode_guid;
			zinom->zi_finfo = ino->zinode_info;
			ZINODE_ULOCK(ino);

			/*
			 * XXX blocking this write will leave
			 * the bucket locked and cause DoS.
			 */
			zctlthr_sendmsgv(fd, zcmh, zinom);
		}
		ULOCK_BUCKET(hb);
	}
}

/*
 * zctlthr_sendreps_gethashtable - respond to a "gethashtable" inquiry.
 *	This computes bucket usage statistics of a hash table and
 *	sends the results back to the client.
 * @fd: client socket descriptor.
 * @zcmh: already filled-in zestion control message header.
 * @zht: hash table message structure to be filled in and sent out.
 */
void
zctlthr_sendreps_gethashtable(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_hashtable *zht)
{
	char name[HTNAME_MAX];
	struct hash_table *ht;
	int found, all;

	snprintf(name, sizeof(name), zht->zht_name);
	all = (strcmp(name, ZHT_NAME_ALL) == 0);

	found = 0;
	spinlock(&hashTablesListLock);
	psclist_for_each_entry(ht, &hashTablesList, htable_entry) {
		if (all || strcmp(name, ht->htable_name) == 0) {
			found = 1;

			snprintf(zht->zht_name, sizeof(zht->zht_name),
			    "%s", ht->htable_name);
			hash_table_stats(ht, &zht->zht_totalbucks,
			    &zht->zht_usedbucks, &zht->zht_nents,
			    &zht->zht_maxbucklen);
			zctlthr_sendmsgv(fd, zcmh, zht);

			if (!all)
				break;
		}
	}
	freelock(&hashTablesListLock);
	if (!found && !all)
		zctlthr_senderrmsg(fd, zcmh,
		    "unknown hash table: %s", name);
}

/*
 * zctlthr_sendrep_getzlc - send a response to a "getzlc" inquiry.
 * @fd: client socket descriptor.
 * @zcmh: already filled-in zestion control message header.
 * @zz: list cache message structure to be filled in and sent out.
 * @zlc: the zest_list_cache about which to reply with information.
 */
void
zctlthr_sendrep_getzlc(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_zlc *zz, list_cache_t *zlc)
{
	if (zlc) {
		snprintf(zz->zz_name, sizeof(zz->zz_name),
		    "%s", zlc->lc_name);
		zz->zz_size = zlc->lc_size;
		zz->zz_max = zlc->lc_max;
		zz->zz_nseen = zlc->lc_nseen;
		LIST_CACHE_ULOCK(zlc);
		zctlthr_sendmsgv(fd, zcmh, zz);
	} else
		zctlthr_senderrmsg(fd, zcmh,
		    "unknown listcache: %s", zz->zz_name);
}

#define MAX_LEVELS 8

void
zctlthr_sendrep_param(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_param *zp, const char *thrname,
    char **levels, int nlevels, const char *value)
{
	char *s, othrname[PSC_THRNAME_MAX];
	const char *p, *end;
	int lvl;

	snprintf(othrname, sizeof(othrname), "%s", zp->zp_thrname);
	snprintf(zp->zp_thrname, sizeof(zp->zp_thrname), "%s", thrname);

	s = zp->zp_field;
	end = s + sizeof(zp->zp_field) - 1;
	for (lvl = 0; s < end && lvl < nlevels; lvl++) {
		for (p = levels[lvl]; s < end && *p; s++, p++)
			*s = *p;
		if (s < end && lvl < nlevels - 1)
			*s++ = '.';
	}
	*s = '\0';

	snprintf(zp->zp_value, sizeof(zp->zp_value), "%s", value);
	zctlthr_sendmsgv(fd, zcmh, zp);

	snprintf(zp->zp_thrname, sizeof(zp->zp_thrname), "%s", othrname);
}

#define FOR_EACH_THREAD(i, zthr, thrname, threads, nthreads)		\
	for ((i) = 0; ((zthr) = (threads)[i]) && (i) < (nthreads); i++)	\
		if (strncmp((zthr)->pscthr_name, (thrname),		\
		    strlen(thrname)) == 0 ||				\
		    strcmp((thrname), ZTHRNAME_EVERYONE) == 0)

void
zctlthr_param_log_level(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_param *zp, char **levels, int nlevels)
{
	int n, nthr, set, loglevel, subsys, start_ss, end_ss;
	struct psc_thread **threads, *zthr;

	levels[0] = "log";
	levels[1] = "level";

	loglevel = 0; /* gcc */
	threads = dynarray_get(&zestionThreads);
	nthr = dynarray_len(&zestionThreads);

	set = (zcmh->zcmh_type == ZCMT_SETPARAM);

	if (set) {
		loglevel = psclog_id(zp->zp_value);
		if (loglevel == -1) {
			zctlthr_senderrmsg(fd, zcmh,
			    "invalid log.level value: %s", zp->zp_value);
			return;
		}
	}

	if (nlevels == 3) {
		/* Subsys specified, use it. */
		subsys = psc_subsys_id(levels[2]);
		if (subsys == -1) {
			zctlthr_senderrmsg(fd, zcmh,
			    "invalid log.level subsystem: %s", levels[2]);
			return;
		}
		start_ss = subsys;
		end_ss = subsys + 1;
	} else {
		/* No subsys specified, use all. */
		start_ss = 0;
		end_ss = ZNSUBSYS;
	}

	FOR_EACH_THREAD(n, zthr, zp->zp_thrname, threads, nthr)
		for (subsys = start_ss; subsys < end_ss; subsys++) {
			levels[2] = psc_subsys_name(subsys);
			if (set)
				zthr->pscthr_loglevels[subsys] = loglevel;
			else {
				zctlthr_sendrep_param(fd, zcmh, zp,
				    zthr->pscthr_name, levels, 3,
				    psclog_name(zthr->pscthr_loglevels[subsys]));
			}
		}
}

void
zctlthr_param_disk_fail(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_param *zp, char **levels, int nlevels)
{
	struct psc_thread **threads, *zthr;
	int n, set, nthr, fail;
	char *s;
	long l;

	nlevels = 2;
	levels[0] = "disk";
	levels[1] = "fail";

	fail = 0; /* gcc */
	threads = dynarray_get(&zestionThreads);
	nthr = dynarray_len(&zestionThreads);

	set = (zcmh->zcmh_type == ZCMT_SETPARAM);

	if (set) {
		l = strtol(zp->zp_value, &s, 10);
		if (l == LONG_MAX || l == LONG_MIN ||
		    *s != '\0' || s == zp->zp_value) {
			zctlthr_senderrmsg(fd, zcmh,
			    "invalid disk.fail value: %s", zp->zp_field);
			return;
		}
		fail = !!l;
	}

	FOR_EACH_THREAD(n, zthr, zp->zp_thrname, threads, nthr) {
		if (zthr->pscthr_type == ZTHRT_IO) {
			if (set)
				ziothr_setfailed(zthr, fail);
			else
				zctlthr_sendrep_param(fd, zcmh, zp,
				    zthr->pscthr_name, levels, nlevels,
				    ziothr(zthr)->zi_disk->zd_failed ?
				    "1" : "0");
		} else if (strcmp(zp->zp_thrname, ZTHRNAME_EVERYONE))
			zctlthr_senderrmsg(fd, zcmh,
			    "not an I/O thread: %s", zthr->pscthr_name);
	}
}

void
zctlthr_param_syncer_enable(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_param *zp, char **levels, int nlevels)
{
	struct psc_thread **threads, *zthr;
	int n, set, nthr, enable;
	char *s;
	long l;

	nlevels = 2;
	levels[0] = "syncer";
	levels[1] = "enable";

	enable = 0; /* gcc */
	threads = dynarray_get(&zestionThreads);
	nthr = dynarray_len(&zestionThreads);

	set = (zcmh->zcmh_type == ZCMT_SETPARAM);

	if (set) {
		l = strtol(zp->zp_value, &s, 10);
		if (l == LONG_MAX || l == LONG_MIN ||
		    *s != '\0' || s == zp->zp_value) {
			zctlthr_senderrmsg(fd, zcmh,
			    "invalid disk.fail value: %s", zp->zp_field);
			return;
		}
		enable = !!l;
	}

	FOR_EACH_THREAD(n, zthr, zp->zp_thrname, threads, nthr) {
		if (zthr->pscthr_type == ZTHRT_SYNCQ) {
			if (set)
				zsyncqthr_setenabled(zthr, enable);
			else
				zctlthr_sendrep_param(fd, zcmh, zp,
				    zthr->pscthr_name, levels, nlevels,
				    zthr->pscthr_run ?  "1" : "0");
		} else if (strcmp(zp->zp_thrname, ZTHRNAME_EVERYONE))
			zctlthr_senderrmsg(fd, zcmh,
			    "not an I/O thread: %s", zthr->pscthr_name);
	}
}

void
zctlthr_sendreps_param(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_param *zp)
{
	char *t, *levels[MAX_LEVELS];
	int nlevels, set;

	set = (zcmh->zcmh_type == ZCMT_SETPARAM);

	for (nlevels = 0, t = zp->zp_field;
	    nlevels < MAX_LEVELS && (levels[nlevels] = t) != NULL;
	    nlevels++) {
		if ((t = strchr(levels[nlevels], '.')) != NULL)
			*t++ = '\0';
		if (*levels[nlevels] == '\0')
			goto invalid;
	}

	if (nlevels == 0 || nlevels >= MAX_LEVELS)
		goto invalid;

	if (strcmp(levels[0], "log") == 0) {
		if (nlevels == 1) {
			if (set)
				goto invalid;
			zctlthr_param_log_level(fd, zcmh, zp, levels, nlevels);
		} else if (strcmp(levels[1], "level") == 0)
			zctlthr_param_log_level(fd, zcmh, zp, levels, nlevels);
		else
			goto invalid;
	} else if (strcmp(levels[0], "disk") == 0) {
		if (nlevels == 1) {
			if (set)
				goto invalid;
			zctlthr_param_disk_fail(fd, zcmh, zp, levels, nlevels);
		} else if (strcmp(levels[1], "fail") == 0)
			zctlthr_param_disk_fail(fd, zcmh, zp, levels, nlevels);
		else
			goto invalid;
	} else if (strcmp(levels[0], "syncer") == 0) {
		if (nlevels == 1) {
			if (set)
				goto invalid;
			zctlthr_param_syncer_enable(fd, zcmh, zp, levels, nlevels);
		} else if (strcmp(levels[1], "enable") == 0)
			zctlthr_param_syncer_enable(fd, zcmh, zp, levels, nlevels);
		else
			goto invalid;
	} else
		goto invalid;
	return;

 invalid:
	while (nlevels > 1)
		levels[--nlevels][-1] = '.';
	zctlthr_senderrmsg(fd, zcmh,
	    "invalid field/value: %s", zp->zp_field);
}

/*
 * zctlthr_sendrep_iostat - send a response to a "getiostat" inquiry.
 * @fd: client socket descriptor.
 * @zcmh: already filled-in zestion control message header.
 * @zist: iostat message structure to be filled in and sent out.
 */
void
zctlthr_sendrep_iostat(int fd, struct zctlmsghdr *zcmh,
    struct zctlmsg_iostats *zist)
{
	char name[IST_NAME_MAX];
	struct iostats *ist;
	int found, all;

	found = 0;
	snprintf(name, sizeof(name), "%s", zist->zist_ist.ist_name);
	all = (strcmp(name, ZIST_NAME_ALL) == 0);

	spinlock(&iostatsListLock);
	psclist_for_each_entry(ist, &iostatsList, ist_lentry)
		if (all ||
		    strncmp(ist->ist_name, name, strlen(name)) == 0) {
			found = 1;

			zist->zist_ist = *ist;
			zctlthr_sendmsgv(fd, zcmh, zist);

			if (strlen(ist->ist_name) == strlen(name))
				break;
		}
	freelock(&iostatsListLock);

	if (!found && !all)
		zctlthr_senderrmsg(fd, zcmh,
		    "unknown iostats: %s", name);
}

/*
 * zctlthr_procmsg - process a message from a client.
 * @fd: client socket descriptor.
 * @zcmh: zestion control message header from client.
 * @zcm: contents of zestion control message from client.
 *
 * Notes: the length of the data buffer `zcm' has not yet
 * been verified for each message type case and must be
 * checked in each case before it can be dereferenced,
 * since there is no way to know until this point.
 */
void
zctlthr_procmsg(int fd, struct zctlmsghdr *zcmh, void *zcm)
{
	struct psc_thread **threads;
	struct zctlmsg_hashtable *zht;
	struct zctlmsg_loglevel *zll;
	struct zctlmsg_iostats *zist;
	struct zctlmsg_inode *zinom;
	struct zctlmsg_meter *zmtr;
	struct zctlmsg_stats *zst;
	struct zctlmsg_mlist *zm;
	struct zctlmsg_param *zp;
	struct zctlmsg_disk *zd;
	struct zctlmsg_zlc *zz;
	struct psclist_head *e;
	int n, nthr;
	size_t j;

	/* XXX lock or snapshot nthreads so it doesn't change underneath us */
	nthr = dynarray_len(&zestionThreads);
	threads = dynarray_get(&zestionThreads);
	switch (zcmh->zcmh_type) {
	case ZCMT_GETLOGLEVEL:
		zll = zcm;
		if (zcmh->zcmh_size != sizeof(*zll))
			goto badlen;
		if (strcasecmp(zll->zll_threadname,
		    ZTHRNAME_EVERYONE) == 0) {
			for (n = 0; n < nthr; n++)
				zctlthr_sendrep_getloglevel(fd,
				    zcmh, zll, threads[n]);
		} else {
			for (n = 0; n < nthr; n++)
				if (strcasecmp(zll->zll_threadname,
				    threads[n]->pscthr_name) == 0) {
					zctlthr_sendrep_getloglevel(fd,
					    zcmh, zll, threads[n]);
					break;
				}
			if (n == nthr)
				zctlthr_senderrmsg(fd, zcmh,
				    "unknown thread: %s",
				    zll->zll_threadname);
		}
		break;
	case ZCMT_GETDISK:
		zd = zcm;
		if (zcmh->zcmh_size != sizeof(*zd))
			goto badlen;
		if (strcasecmp(zd->zd_threadname,
		    ZTHRNAME_EVERYONE) == 0) {
			for (j = 0; j < zestionNIOThreads; j++)
				zctlthr_sendrep_getdisk(fd, zcmh,
				    zestionIOThreads[j]);
		} else {
			for (j = 0; j < zestionNIOThreads; j++)
				if (strcasecmp(zd->zd_threadname,
				    zestionIOThreads[j]->pscthr_name) == 0) {
					zctlthr_sendrep_getdisk(fd,
					    zcmh, zestionIOThreads[j]);
					break;
				}
			if (j == zestionNIOThreads)
				zctlthr_senderrmsg(fd, zcmh,
				    "unknown I/O thread: %s",
				    zd->zd_threadname);
		}
		break;
	case ZCMT_GETINODE:
		zinom = zcm;
		if (zcmh->zcmh_size != sizeof(*zinom))
			goto badlen;
		zctlthr_sendrep_getinode(fd, zcmh, zinom);
		break;
	case ZCMT_GETHASHTABLE: {
		zht = zcm;
		if (zcmh->zcmh_size != sizeof(*zht))
			goto badlen;
		zctlthr_sendreps_gethashtable(fd, zcmh, zht);
		break;
	    }
	case ZCMT_GETMLIST:
		zm = zcm;
		if (zcmh->zcmh_size != sizeof(*zm))
			goto badlen;
		zctlthr_sendreps_getmlist(fd, zcmh, zm);
		break;
	case ZCMT_GETMETER:
		zmtr = zcm;
		if (zcmh->zcmh_size != sizeof(*zmtr))
			goto badlen;
		zctlthr_sendreps_getmeter(fd, zcmh, zmtr);
		break;
	case ZCMT_GETSTATS:
		zst = zcm;
		if (zcmh->zcmh_size != sizeof(*zst))
			goto badlen;
		if (strcasecmp(zst->zst_threadname,
		    ZTHRNAME_EVERYONE) == 0) {
			for (n = 0; n < nthr; n++)
				zctlthr_sendrep_getstats(fd,
				    zcmh, zst, threads[n], 1);
		} else {
			for (n = 0; n < nthr; n++)
				if (strcasecmp(zst->zst_threadname,
				    threads[n]->pscthr_name) == 0) {
					zctlthr_sendrep_getstats(fd,
					    zcmh, zst, threads[n], 0);
					break;
				}
			if (n == nthr)
				zctlthr_senderrmsg(fd, zcmh,
				    "unknown thread: %s",
				    zst->zst_threadname);
		}
		break;
	case ZCMT_GETZLC:
		zz = zcm;
		if (zcmh->zcmh_size != sizeof(*zz))
			goto badlen;
		if (strcmp(zz->zz_name, ZLC_NAME_ALL) == 0) {
			list_cache_t *zlc;

			spinlock(&zestListCachesLock);
			psclist_for_each(e, &zestListCaches) {
				zlc = psclist_entry(e, list_cache_t,
				    lc_index_lentry);
				LIST_CACHE_LOCK(zlc);
				zctlthr_sendrep_getzlc(fd, zcmh, zz, zlc);
			}
			freelock(&zestListCachesLock);
		} else
			zctlthr_sendrep_getzlc(fd, zcmh, zz,
			    lc_lookup(zz->zz_name));
		break;
	case ZCMT_GETPARAM:
	case ZCMT_SETPARAM:
		zp = zcm;
		if (zcmh->zcmh_size != sizeof(*zp))
			goto badlen;
		zctlthr_sendreps_param(fd, zcmh, zp);
		break;
	case ZCMT_GETIOSTAT:
		zist = zcm;
		if (zcmh->zcmh_size != sizeof(*zist))
			goto badlen;
		zctlthr_sendrep_iostat(fd, zcmh, zist);
		break;
	default:
		warnx("unexpected msg type; type=%d size=%zu",
		    zcmh->zcmh_type, zcmh->zcmh_size);
		break;
	}
	return;
 badlen:
	warnx("unexpected msg size; type=%d, siz=%zu", zcmh->zcmh_type,
	    zcmh->zcmh_size);
}

/*
 * zctlthr_service - satisfy a client connection.
 * @fd: client socket descriptor.
 *
 * Notes: sched_yield() is not explicity called throughout this routine,
 * which has implications, advantages, and disadvantages.
 *
 * Implications: we run till we finish the client connection and the next
 * accept() puts us back to sleep, if no intervening system calls which
 * run in the meantime relinquish control to other threads.
 *
 * Advantages: it might be nice to block all threads so processing by
 * other threads doesn't happen while control messages which modify
 * zestiond operation are being processed.
 *
 * Disadvantages: if we don't go to sleep during processing of client
 * connection, anyone can denial the zestion service quite easily.
 */
void
zctlthr_service(int fd)
{
	struct zctlmsghdr zcmh;
	size_t zcmsiz;
	void *zcm;
	ssize_t n;

	zcm = NULL;
	zcmsiz = 0;
	while ((n = read(fd, &zcmh, sizeof(zcmh))) != -1 && n != 0) {
		if (n != sizeof(zcmh)) {
			psc_notice("short read on zctlmsghdr; read=%zd", n);
			continue;
		}
		if (zcmh.zcmh_size == 0) {
			psc_warnx("empty zctlmsg; type=%d", zcmh.zcmh_type);
			continue;
		}
		if (zcmh.zcmh_size > zcmsiz) {
			zcmsiz = zcmh.zcmh_size;
			if ((zcm = realloc(zcm, zcmsiz)) == NULL)
				err(1, "realloc");
		}
		n = read(fd, zcm, zcmh.zcmh_size);
		if (n == -1)
			err(1, "read");
		if ((size_t)n != zcmh.zcmh_size) {
			psc_warn("short read on zctlmsg contents; "
			    "read=%zu; expected=%zu",
			    n, zcmh.zcmh_size);
			break;
		}
		zctlthr_procmsg(fd, &zcmh, zcm);
		sched_yield();
	}
	if (n == -1)
		err(1, "read");
	free(zcm);
}

/*
 * zctlthr_main - main zestion control thread client-servicing loop.
 * @arg: zestion thread structure.
 */
__dead void
zctlthr_main(const char *fn)
{
	struct sockaddr_un sun;
	mode_t old_umask;
	sigset_t sigset;
	socklen_t siz;
	int rc, s, fd;

	if (sigemptyset(&sigset) == -1)
		psc_fatal("sigemptyset");
	if (sigaddset(&sigset, SIGPIPE) == -1)
		psc_fatal("sigemptyset");
	rc = pthread_sigmask(SIG_BLOCK, &sigset, NULL);
	if (rc)
		psc_fatalx("pthread_sigmask: %s", strerror(rc));

	if ((s = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
		psc_fatal("socket");

	bzero(&sun, sizeof(sun));
	sun.sun_family = AF_UNIX;
	snprintf(sun.sun_path, sizeof(sun.sun_path), "%s", fn);
	if (unlink(fn) == -1)
		if (errno != ENOENT)
			psc_error("unlink %s", fn);

	old_umask = umask(S_IXUSR | S_IXGRP | S_IWOTH | S_IROTH | S_IXOTH);
	if (bind(s, (struct sockaddr *)&sun, sizeof(sun)) == -1)
		psc_fatal("bind %s", fn);
	umask(old_umask);

	/* XXX fchmod */
	if (chmod(fn, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP |
	    S_IROTH | S_IWOTH) == -1) {
		unlink(fn);
		psc_fatal("chmod %s", fn);
	}

	if (listen(s, Q) == -1)
		psc_fatal("listen");

	for (;;) {
		siz = sizeof(sun);
		if ((fd = accept(s, (struct sockaddr *)&sun,
		    &siz)) == -1)
			err(1, "accept");
		zctlthr(&zestionControlThread)->zc_st_nclients++;
		zctlthr_service(fd);
		close(fd);
	}
	/* NOTREACHED */
}

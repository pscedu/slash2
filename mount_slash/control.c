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

struct psc_thread ctlthr;

#define Q 15	/* listen() queue */

/*
 * msctlthr_sendmsgv - send a control message back to client.
 * @fd: client socket descriptor.
 * @smch: already filled-out control message header.
 * @m: control message contents.
 */
void
msctlthr_sendmsgv(int fd, const struct msctlmsghdr *mh, const void *m)
{
	struct iovec iov[2];
	size_t tsiz;
	ssize_t n;

	iov[0].iov_base = (void *)mh;
	iov[0].iov_len = sizeof(*mh);

	iov[1].iov_base = (void *)m;
	iov[1].iov_len = mh->mh_size;

	n = writev(fd, iov, NENTRIES(iov));
	if (n == -1)
		psc_fatal("write");
	tsiz = sizeof(*mh) + mh->mh_size;
	if ((size_t)n != tsiz)
		warn("short write");
	msctlthr(&ctlthr)->mc_st_nsent++;
	sched_yield();
}

/*
 * msctlthr_sendmsg - send a control message back to client.
 * @fd: client socket descriptor.
 * @type: type of message.
 * @siz: size of message.
 * @m: control message contents.
 * Notes: a control message header will be constructed and
 * written to the client preceding the message contents.
 */
void
msctlthr_sendmsg(int fd, int type, size_t siz, const void *m)
{
	struct msctlmsghdr mh;
	struct iovec iov[2];
	size_t tsiz;
	ssize_t n;

	memset(&mh, 0, sizeof(mh));
	mh.mh_type = type;
	mh.mh_size = siz;

	iov[0].iov_base = &mh;
	iov[0].iov_len = sizeof(mh);

	iov[1].iov_base = (void *)m;
	iov[1].iov_len = siz;

	n = writev(fd, iov, NENTRIES(iov));
	if (n == -1)
		psc_fatal("write");
	tsiz = sizeof(mh) + siz;
	if ((size_t)n != tsiz)
		warn("short write");
	msctlthr(&ctlthr)->mc_st_nsent++;
	sched_yield();
}

/*
 * msctlthr_senderror - send an error to client.
 * @fd: client socket descriptor.
 * @fmt: printf(3) format of error message.
 */
void
msctlthr_senderror(int fd, struct msctlmsghdr *mh, const char *fmt, ...)
{
	struct msctlmsg_error me;
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(me.me_errmsg, sizeof(me.me_errmsg), fmt, ap); /* XXX */
	va_end(ap);

	mh->mh_type = MSCMT_ERROR;
	mh->mh_size = sizeof(me);
	msctlthr_sendmsgv(fd, mh, &me);
}

/*
 * msctlthr_sendrep_getstats - send a response to a "getstats" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @mst: thread stats message structure to be filled in and sent out.
 * @thr: thread begin queried.
 * @probe: whether to send empty msgs for threads which do not track stats.
 */
void
msctlthr_sendrep_getstats(int fd, struct msctlmsghdr *mh,
    struct msctlmsg_stats *mst, struct psc_thread *thr, int probe)
{
	snprintf(mst->mst_thrname, sizeof(mst->mst_thrname),
	    "%s", thr->pscthr_name);
	mst->mst_thrtype = thr->pscthr_type;
	switch (thr->pscthr_type) {
	case MSTHRT_CTL:
		mst->mst_nclients = msctlthr(thr)->mc_st_nclients;
		mst->mst_nsent    = msctlthr(thr)->mc_st_nsent;
		mst->mst_nrecv    = msctlthr(thr)->mc_st_nrecv;
		break;
	default:
		if (probe)
			return;
		break;
	}
	msctlthr_sendmsgv(fd, mh, mst);
}

/*
 * msctlthr_sendrep_getsubsys - send a response to a "getsubsys" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 */
void
msctlthr_sendrep_getsubsys(int fd, struct msctlmsghdr *mh)
{
	struct msctlmsg_subsys *ms;
	const char **ss;
	size_t siz;
	int n;

	siz = MSS_NAME_MAX * psc_nsubsys;
	ms = PSCALLOC(siz);
	ss = dynarray_get(&psc_subsystems);
	for (n = 0; n < psc_nsubsys; n++)
		if (snprintf(&ms->ms_names[n * MSS_NAME_MAX],
		    MSS_NAME_MAX, "%s", ss[n]) == -1) {
			psc_warn("snprintf");
			msctlthr_senderror(fd, mh,
			    "unable to retrieve subsystems");
			goto done;
		}
	mh->mh_size = siz;
	msctlthr_sendmsgv(fd, mh, ms);
 done:
	mh->mh_size = 0;	/* reset because we used our own buffer */
	free(ms);
}

/*
 * msctlthr_sendrep_getloglevel - send a response to a "getloglevel" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @thr: thread begin queried.
 */
void
msctlthr_sendrep_getloglevel(int fd, struct msctlmsghdr *mh,
    struct psc_thread *thr)
{
	struct msctlmsg_loglevel *ml;
	size_t siz;

	siz = sizeof(*ml) + sizeof(*ml->ml_levels) * psc_nsubsys;
	ml = PSCALLOC(siz);
	snprintf(ml->ml_thrname, sizeof(ml->ml_thrname),
	    "%s", thr->pscthr_name);
	memcpy(ml->ml_levels, thr->pscthr_loglevels, psc_nsubsys *
	    sizeof(*ml->ml_levels));
	mh->mh_size = siz;
	msctlthr_sendmsgv(fd, mh, ml);
	mh->mh_size = 0;	/* reset because we used our own buffer */
	free(ml);
}

/*
 * msctlthr_sendreps_gethashtable - respond to a "gethashtable" inquiry.
 *	This computes bucket usage statistics of a hash table and
 *	sends the results back to the client.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @mht: hash table message structure to be filled in and sent out.
 */
void
msctlthr_sendreps_gethashtable(int fd, struct msctlmsghdr *mh,
    struct msctlmsg_hashtable *mht)
{
	char name[HTNAME_MAX];
	struct hash_table *ht;
	int found, all;

	snprintf(name, sizeof(name), mht->mht_name);
	all = (strcmp(name, MSHT_NAME_ALL) == 0);

	found = 0;
	spinlock(&hashTablesListLock);
	psclist_for_each_entry(ht, &hashTablesList, htable_entry) {
		if (all || strcmp(name, ht->htable_name) == 0) {
			found = 1;

			snprintf(mht->mht_name, sizeof(mht->mht_name),
			    "%s", ht->htable_name);
			hash_table_stats(ht, &mht->mht_totalbucks,
			    &mht->mht_usedbucks, &mht->mht_nents,
			    &mht->mht_maxbucklen);
			msctlthr_sendmsgv(fd, mh, mht);

			if (!all)
				break;
		}
	}
	freelock(&hashTablesListLock);
	if (!found && !all)
		msctlthr_senderror(fd, mh,
		    "unknown hash table: %s", name);
}

/*
 * msctlthr_sendrep_getlc - send a response to a "getlc" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @mlc: list cache message structure to be filled in and sent out.
 * @lc: the list_cache about which to reply with information.
 */
void
msctlthr_sendrep_getlc(int fd, struct msctlmsghdr *mh,
    struct msctlmsg_lc *mlc, list_cache_t *lc)
{
	if (lc) {
		snprintf(mlc->mlc_name, sizeof(mlc->mlc_name),
		    "%s", lc->lc_name);
		mlc->mlc_size = lc->lc_size;
		mlc->mlc_max = lc->lc_max;
		mlc->mlc_nseen = lc->lc_nseen;
		LIST_CACHE_ULOCK(lc);
		msctlthr_sendmsgv(fd, mh, mlc);
	} else
		msctlthr_senderror(fd, mh,
		    "unknown listcache: %s", mlc->mlc_name);
}

#define MAX_LEVELS 8

void
msctlthr_sendrep_param(int fd, struct msctlmsghdr *mh,
    struct msctlmsg_param *mp, const char *thrname,
    char **levels, int nlevels, const char *value)
{
	char *s, othrname[PSC_THRNAME_MAX];
	const char *p, *end;
	int lvl;

	snprintf(othrname, sizeof(othrname), "%s", mp->mp_thrname);
	snprintf(mp->mp_thrname, sizeof(mp->mp_thrname), "%s", thrname);

	s = mp->mp_field;
	end = s + sizeof(mp->mp_field) - 1;
	for (lvl = 0; s < end && lvl < nlevels; lvl++) {
		for (p = levels[lvl]; s < end && *p; s++, p++)
			*s = *p;
		if (s < end && lvl < nlevels - 1)
			*s++ = '.';
	}
	*s = '\0';

	snprintf(mp->mp_value, sizeof(mp->mp_value), "%s", value);
	msctlthr_sendmsgv(fd, mh, mp);

	snprintf(mp->mp_thrname, sizeof(mp->mp_thrname), "%s", othrname);
}

#define FOR_EACH_THREAD(i, thr, thrname, threads, nthreads)		\
	for ((i) = 0; ((thr) = (threads)[i]) && (i) < (nthreads); i++)	\
		if (strncmp((thr)->pscthr_name, (thrname),		\
		    strlen(thrname)) == 0 ||				\
		    strcmp((thrname), MSTHRNAME_EVERYONE) == 0)

void
msctlthr_param_log_level(int fd, struct msctlmsghdr *mh,
    struct msctlmsg_param *mp, char **levels, int nlevels)
{
	int n, nthr, set, loglevel, subsys, start_ss, end_ss;
	struct psc_thread **threads, *thr;

	levels[0] = "log";
	levels[1] = "level";

	loglevel = 0; /* gcc */
	threads = dynarray_get(&pscThreads);
	nthr = dynarray_len(&pscThreads);

	set = (mh->mh_type == MSCMT_SETPARAM);

	if (set) {
		loglevel = psclog_id(mp->mp_value);
		if (loglevel == -1) {
			msctlthr_senderror(fd, mh,
			    "invalid log.level value: %s", mp->mp_value);
			return;
		}
	}

	if (nlevels == 3) {
		/* Subsys specified, use it. */
		subsys = psc_subsys_id(levels[2]);
		if (subsys == -1) {
			msctlthr_senderror(fd, mh,
			    "invalid log.level subsystem: %s", levels[2]);
			return;
		}
		start_ss = subsys;
		end_ss = subsys + 1;
	} else {
		/* No subsys specified, use all. */
		start_ss = 0;
		end_ss = psc_nsubsys;
	}

	FOR_EACH_THREAD(n, thr, mp->mp_thrname, threads, nthr)
		for (subsys = start_ss; subsys < end_ss; subsys++) {
			levels[2] = psc_subsys_name(subsys);
			if (set)
				thr->pscthr_loglevels[subsys] = loglevel;
			else {
				msctlthr_sendrep_param(fd, mh, mp,
				    thr->pscthr_name, levels, 3,
				    psclog_name(thr->pscthr_loglevels[subsys]));
			}
		}
}

void
msctlthr_sendreps_param(int fd, struct msctlmsghdr *mh,
    struct msctlmsg_param *mp)
{
	char *t, *levels[MAX_LEVELS];
	int nlevels, set;

	set = (mh->mh_type == MSCMT_SETPARAM);

	for (nlevels = 0, t = mp->mp_field;
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
			msctlthr_param_log_level(fd, mh, mp, levels, nlevels);
		} else if (strcmp(levels[1], "level") == 0)
			msctlthr_param_log_level(fd, mh, mp, levels, nlevels);
		else
			goto invalid;
	} else
		goto invalid;
	return;

 invalid:
	while (nlevels > 1)
		levels[--nlevels][-1] = '.';
	msctlthr_senderror(fd, mh,
	    "invalid field/value: %s", mp->mp_field);
}

#if 0
/*
 * msctlthr_sendrep_iostat - send a response to a "getiostat" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @sist: iostat message structure to be filled in and sent out.
 */
void
msctlthr_sendrep_iostat(int fd, struct msctlmsghdr *mh,
    struct msctlmsg_iostats *sist)
{
	char name[IST_NAME_MAX];
	struct iostats *ist;
	int found, all;

	found = 0;
	snprintf(name, sizeof(name), "%s", sist->sist_ist.ist_name);
	all = (strcmp(name, SIST_NAME_ALL) == 0);

	spinlock(&iostatsListLock);
	psclist_for_each_entry(ist, &iostatsList, ist_lentry)
		if (all ||
		    strncmp(ist->ist_name, name, strlen(name)) == 0) {
			found = 1;

			sist->sist_ist = *ist;
			msctlthr_sendmsgv(fd, mh, sist);

			if (strlen(ist->ist_name) == strlen(name))
				break;
		}
	freelock(&iostatsListLock);

	if (!found && !all)
		msctlthr_senderror(fd, mh,
		    "unknown iostats: %s", name);
}
#endif

/*
 * msctlthr_procmsg - process a message from a client.
 * @fd: client socket descriptor.
 * @mh: control message header from client.
 * @m: contents of control message from client.
 *
 * Notes: the length of the data buffer `m' has not yet
 * been verified for each message type case and must be
 * checked in each case before it can be dereferenced,
 * since there is no way to know until this point.
 */
void
msctlthr_procmsg(int fd, struct msctlmsghdr *mh, void *m)
{
	struct psc_thread **threads;
	struct msctlmsg_hashtable *mht;
	struct msctlmsg_loglevel *ml;
	struct msctlmsg_stats *mst;
	struct msctlmsg_param *mp;
	struct msctlmsg_lc *mlc;
	int n, nthr;

	/* XXX lock or snapshot nthreads so it doesn't change underneath us */
	nthr = dynarray_len(&pscThreads);
	threads = dynarray_get(&pscThreads);
	switch (mh->mh_type) {
	case MSCMT_GETSUBSYS:
		msctlthr_sendrep_getsubsys(fd, mh);
		break;
	case MSCMT_GETLOGLEVEL:
		ml = m;
		if (mh->mh_size != sizeof(*ml))
			goto badlen;
		if (strcasecmp(ml->ml_thrname,
		    MSTHRNAME_EVERYONE) == 0) {
			for (n = 0; n < nthr; n++)
				msctlthr_sendrep_getloglevel(fd,
				    mh, threads[n]);
		} else {
			for (n = 0; n < nthr; n++)
				if (strcasecmp(ml->ml_thrname,
				    threads[n]->pscthr_name) == 0) {
					msctlthr_sendrep_getloglevel(fd,
					    mh, threads[n]);
					break;
				}
			if (n == nthr)
				msctlthr_senderror(fd, mh,
				    "unknown thread: %s",
				    ml->ml_thrname);
		}
		break;
	case MSCMT_GETHASHTABLE:
		mht = m;
		if (mh->mh_size != sizeof(*mht))
			goto badlen;
		msctlthr_sendreps_gethashtable(fd, mh, mht);
		break;
	case MSCMT_GETSTATS:
		mst = m;
		if (mh->mh_size != sizeof(*mst))
			goto badlen;
		if (strcasecmp(mst->mst_thrname,
		    MSTHRNAME_EVERYONE) == 0) {
			for (n = 0; n < nthr; n++)
				msctlthr_sendrep_getstats(fd,
				    mh, mst, threads[n], 1);
		} else {
			for (n = 0; n < nthr; n++)
				if (strcasecmp(mst->mst_thrname,
				    threads[n]->pscthr_name) == 0) {
					msctlthr_sendrep_getstats(fd,
					    mh, mst, threads[n], 0);
					break;
				}
			if (n == nthr)
				msctlthr_senderror(fd, mh,
				    "unknown thread: %s",
				    mst->mst_thrname);
		}
		break;
	case MSCMT_GETLC:
		mlc = m;
		if (mh->mh_size != sizeof(*mlc))
			goto badlen;
		if (strcmp(mlc->mlc_name, MSLC_NAME_ALL) == 0) {
			list_cache_t *lc;

			spinlock(&pscListCachesLock);
			psclist_for_each_entry(lc, &pscListCaches,
			    lc_index_lentry) {
				LIST_CACHE_LOCK(lc);
				msctlthr_sendrep_getlc(fd, mh, mlc, lc);
			}
			freelock(&pscListCachesLock);
		} else
			msctlthr_sendrep_getlc(fd, mh, mlc,
			    lc_lookup(mlc->mlc_name));
		break;
	case MSCMT_GETPARAM:
	case MSCMT_SETPARAM:
		mp = m;
		if (mh->mh_size != sizeof(*mp))
			goto badlen;
		msctlthr_sendreps_param(fd, mh, mp);
		break;
#if 0
	case MSCMT_GETIOSTAT:
		sist = m;
		if (mh->mh_size != sizeof(*sist))
			goto badlen;
		msctlthr_sendrep_iostat(fd, mh, sist);
		break;
#endif
	default:
		warnx("unexpected msg type; type=%d size=%zu",
		    mh->mh_type, mh->mh_size);
		break;
	}
	return;
 badlen:
	warnx("unexpected msg size; type=%d, siz=%zu", mh->mh_type,
	    mh->mh_size);
}

/*
 * msctlthr_service - satisfy a client connection.
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
 * mount_slash operation are being processed.
 *
 * Disadvantages: if we don't go to sleep during processing of client
 * connection, anyone can denial the service quite easily.
 */
void
msctlthr_service(int fd)
{
	struct msctlmsghdr mh;
	size_t siz;
	ssize_t n;
	void *m;

	m = NULL;
	siz = 0;
	while ((n = read(fd, &mh, sizeof(mh))) != -1 && n != 0) {
		if (n != sizeof(mh)) {
			psc_notice("short read on msctlmsghdr; read=%zd", n);
			continue;
		}
		if (mh.mh_size > siz) {
			siz = mh.mh_size;
			if ((m = realloc(m, siz)) == NULL)
				psc_fatal("realloc");
		}
		n = read(fd, m, mh.mh_size);
		if (n == -1)
			psc_fatal("read");
		if ((size_t)n != mh.mh_size) {
			psc_warn("short read on msctlmsg contents; "
			    "read=%zu; expected=%zu",
			    n, mh.mh_size);
			break;
		}
		msctlthr_procmsg(fd, &mh, m);
	}
	if (n == -1)
		psc_fatal("read");
	free(m);
}

/*
 * msctlthr_main - main control thread client-servicing loop.
 * @fn: path to control socket.
 */
__dead void
msctlthr_main(const char *fn)
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
			psc_fatal("accept");
		msctlthr(&ctlthr)->mc_st_nclients++;
		msctlthr_service(fd);
		close(fd);
	}
	/* NOTREACHED */
}

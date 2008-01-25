/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a currently-running slashd.
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

#include "inode.h"
#include "control.h"
#include "slash.h"

struct psc_thread slashControlThread;

#define Q 15	/* listen() queue */

/*
 * slctlthr_sendmsgv - send a control message back to client.
 * @fd: client socket descriptor.
 * @smch: already filled-out slash control message header.
 * @scm: slash control message contents.
 */
void
slctlthr_sendmsgv(int fd, const struct slctlmsghdr *scmh, const void *scm)
{
	struct iovec iov[2];
	size_t tsiz;
	ssize_t n;

	iov[0].iov_base = (void *)scmh;
	iov[0].iov_len = sizeof(*scmh);

	iov[1].iov_base = (void *)scm;
	iov[1].iov_len = scmh->scmh_size;

	n = writev(fd, iov, NENTRIES(iov));
	if (n == -1)
		err(1, "write");
	tsiz = sizeof(*scmh) + scmh->scmh_size;
	if ((size_t)n != tsiz)
		warn("short write");
	slctlthr(&slashControlThread)->sc_st_nsent++;
	sched_yield();
}

/*
 * slctlthr_sendmsg - send a control message back to client.
 * @fd: client socket descriptor.
 * @type: type of message.
 * @siz: size of message.
 * @scm: slash control message contents.
 * Notes: a slash control message header will be constructed and
 * written to the client preceding the message contents.
 */
void
slctlthr_sendmsg(int fd, int type, size_t siz, const void *scm)
{
	struct slctlmsghdr scmh;
	struct iovec iov[2];
	size_t tsiz;
	ssize_t n;

	memset(&scmh, 0, sizeof(scmh));
	scmh.scmh_type = type;
	scmh.scmh_size = siz;

	iov[0].iov_base = &scmh;
	iov[0].iov_len = sizeof(scmh);

	iov[1].iov_base = (void *)scm;
	iov[1].iov_len = siz;

	n = writev(fd, iov, NENTRIES(iov));
	if (n == -1)
		err(1, "write");
	tsiz = sizeof(scmh) + siz;
	if ((size_t)n != tsiz)
		warn("short write");
	slctlthr(&slashControlThread)->sc_st_nsent++;
	sched_yield();
}

/*
 * slctlthr_senderrmsg - send an error message to client.
 * @fd: client socket descriptor.
 * @fmt: printf(3) format of error message.
 */
void
slctlthr_senderrmsg(int fd, struct slctlmsghdr *scmh, const char *fmt, ...)
{
	struct slctlmsg_errmsg sem;
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(sem.sem_errmsg, sizeof(sem.sem_errmsg), fmt, ap);
	va_end(ap);

	scmh->scmh_type = SCMT_ERRMSG;
	scmh->scmh_size = sizeof(sem);
	slctlthr_sendmsgv(fd, scmh, &sem);
}

/*
 * slctlthr_sendrep_getstats - send a response to a "getstats" inquiry.
 * @fd: client socket descriptor.
 * @scmh: already filled-in slash control message header.
 * @sst: thread stats message structure to be filled in and sent out.
 * @thr: slash thread begin queried.
 * @probe: whether to send empty msgs for threads which do not track stats.
 */
void
slctlthr_sendrep_getstats(int fd, struct slctlmsghdr *scmh,
    struct slctlmsg_stats *sst, struct psc_thread *thr, int probe)
{
	snprintf(sst->sst_thrname, sizeof(sst->sst_thrname),
	    "%s", thr->pscthr_name);
	sst->sst_thrtype = thr->pscthr_type;
	switch (thr->pscthr_type) {
	case SLTHRT_CTL:
		sst->sst_nclients = slctlthr(thr)->sc_st_nclients;
		sst->sst_nsent    = slctlthr(thr)->sc_st_nsent;
		sst->sst_nrecv    = slctlthr(thr)->sc_st_nrecv;
		break;
	default:
		if (probe)
			return;
		break;
	}
	slctlthr_sendmsgv(fd, scmh, sst);
}

/*
 * slctlthr_sendrep_getsubsys - send a response to a "getsubsys" inquiry.
 * @fd: client socket descriptor.
 * @scmh: already filled-in slash control message header.
 */
void
slctlthr_sendrep_getsubsys(int fd, struct slctlmsghdr *scmh)
{
	struct slctlmsg_subsys *sss;
	const char **ss;
	size_t siz;
	int n, rc;

	siz = SSS_NAME_MAX * psc_nsubsys;
	sss = PSCALLOC(siz);
	ss = dynarray_get(&psc_subsystems);
	for (n = 0; n < psc_nsubsys; n++)
		if ((rc = snprintf(&sss->sss_names[n * SSS_NAME_MAX],
		    SSS_NAME_MAX, "%s", ss[n])) == -1) {
			psc_warn("snprintf");
			slctlthr_senderrmsg(fd, scmh,
			    "unable to retrieve subsystems");
			goto done;
		}
	scmh->scmh_size = siz;
	slctlthr_sendmsgv(fd, scmh, sss);
 done:
	free(sss);
}

/*
 * slctlthr_sendrep_getloglevel - send a response to a "getloglevel" inquiry.
 * @fd: client socket descriptor.
 * @scmh: already filled-in slash control message header.
 * @sll: loglevel message structure to be filled in and sent out.
 * @thr: slash thread begin queried.
 */
void
slctlthr_sendrep_getloglevel(int fd, struct slctlmsghdr *scmh,
    struct slctlmsg_loglevel *sll, struct psc_thread *thr)
{
	snprintf(sll->sll_thrname, sizeof(sll->sll_thrname),
	    "%s", thr->pscthr_name);
	memcpy(sll->sll_levels, thr->pscthr_loglevels, psc_nsubsys *
	    sizeof(*sll->sll_levels));
	slctlthr_sendmsgv(fd, scmh, sll);
}

/*
 * slctlthr_sendreps_gethashtable - respond to a "gethashtable" inquiry.
 *	This computes bucket usage statistics of a hash table and
 *	sends the results back to the client.
 * @fd: client socket descriptor.
 * @scmh: already filled-in slash control message header.
 * @sht: hash table message structure to be filled in and sent out.
 */
void
slctlthr_sendreps_gethashtable(int fd, struct slctlmsghdr *scmh,
    struct slctlmsg_hashtable *sht)
{
	char name[HTNAME_MAX];
	struct hash_table *ht;
	int found, all;

	snprintf(name, sizeof(name), sht->sht_name);
	all = (strcmp(name, SHT_NAME_ALL) == 0);

	found = 0;
	spinlock(&hashTablesListLock);
	psclist_for_each_entry(ht, &hashTablesList, htable_entry) {
		if (all || strcmp(name, ht->htable_name) == 0) {
			found = 1;

			snprintf(sht->sht_name, sizeof(sht->sht_name),
			    "%s", ht->htable_name);
			hash_table_stats(ht, &sht->sht_totalbucks,
			    &sht->sht_usedbucks, &sht->sht_nents,
			    &sht->sht_maxbucklen);
			slctlthr_sendmsgv(fd, scmh, sht);

			if (!all)
				break;
		}
	}
	freelock(&hashTablesListLock);
	if (!found && !all)
		slctlthr_senderrmsg(fd, scmh,
		    "unknown hash table: %s", name);
}

/*
 * slctlthr_sendrep_getlc - send a response to a "getlc" inquiry.
 * @fd: client socket descriptor.
 * @scmh: already filled-in slash control message header.
 * @slc: list cache message structure to be filled in and sent out.
 * @lc: the list_cache about which to reply with information.
 */
void
slctlthr_sendrep_getlc(int fd, struct slctlmsghdr *scmh,
    struct slctlmsg_lc *slc, list_cache_t *lc)
{
	if (lc) {
		snprintf(slc->slc_name, sizeof(slc->slc_name),
		    "%s", lc->lc_name);
		slc->slc_size = lc->lc_size;
		slc->slc_max = lc->lc_max;
		slc->slc_nseen = lc->lc_nseen;
		LIST_CACHE_ULOCK(lc);
		slctlthr_sendmsgv(fd, scmh, slc);
	} else
		slctlthr_senderrmsg(fd, scmh,
		    "unknown listcache: %s", slc->slc_name);
}

#define MAX_LEVELS 8

void
slctlthr_sendrep_param(int fd, struct slctlmsghdr *scmh,
    struct slctlmsg_param *sp, const char *thrname,
    char **levels, int nlevels, const char *value)
{
	char *s, othrname[PSC_THRNAME_MAX];
	const char *p, *end;
	int lvl;

	snprintf(othrname, sizeof(othrname), "%s", sp->sp_thrname);
	snprintf(sp->sp_thrname, sizeof(sp->sp_thrname), "%s", thrname);

	s = sp->sp_field;
	end = s + sizeof(sp->sp_field) - 1;
	for (lvl = 0; s < end && lvl < nlevels; lvl++) {
		for (p = levels[lvl]; s < end && *p; s++, p++)
			*s = *p;
		if (s < end && lvl < nlevels - 1)
			*s++ = '.';
	}
	*s = '\0';

	snprintf(sp->sp_value, sizeof(sp->sp_value), "%s", value);
	slctlthr_sendmsgv(fd, scmh, sp);

	snprintf(sp->sp_thrname, sizeof(sp->sp_thrname), "%s", othrname);
}

#define FOR_EACH_THREAD(i, thr, thrname, threads, nthreads)		\
	for ((i) = 0; ((thr) = (threads)[i]) && (i) < (nthreads); i++)	\
		if (strncmp((thr)->pscthr_name, (thrname),		\
		    strlen(thrname)) == 0 ||				\
		    strcmp((thrname), STHRNAME_EVERYONE) == 0)

void
slctlthr_param_log_level(int fd, struct slctlmsghdr *scmh,
    struct slctlmsg_param *sp, char **levels, int nlevels)
{
	int n, nthr, set, loglevel, subsys, start_ss, end_ss;
	struct psc_thread **threads, *thr;

	levels[0] = "log";
	levels[1] = "level";

	loglevel = 0; /* gcc */
	threads = dynarray_get(&pscThreads);
	nthr = dynarray_len(&pscThreads);

	set = (scmh->scmh_type == SCMT_SETPARAM);

	if (set) {
		loglevel = psclog_id(sp->sp_value);
		if (loglevel == -1) {
			slctlthr_senderrmsg(fd, scmh,
			    "invalid log.level value: %s", sp->sp_value);
			return;
		}
	}

	if (nlevels == 3) {
		/* Subsys specified, use it. */
		subsys = psc_subsys_id(levels[2]);
		if (subsys == -1) {
			slctlthr_senderrmsg(fd, scmh,
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

	FOR_EACH_THREAD(n, thr, sp->sp_thrname, threads, nthr)
		for (subsys = start_ss; subsys < end_ss; subsys++) {
			levels[2] = psc_subsys_name(subsys);
			if (set)
				thr->pscthr_loglevels[subsys] = loglevel;
			else {
				slctlthr_sendrep_param(fd, scmh, sp,
				    thr->pscthr_name, levels, 3,
				    psclog_name(thr->pscthr_loglevels[subsys]));
			}
		}
}

void
slctlthr_sendreps_param(int fd, struct slctlmsghdr *scmh,
    struct slctlmsg_param *sp)
{
	char *t, *levels[MAX_LEVELS];
	int nlevels, set;

	set = (scmh->scmh_type == SCMT_SETPARAM);

	for (nlevels = 0, t = sp->sp_field;
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
			slctlthr_param_log_level(fd, scmh, sp, levels, nlevels);
		} else if (strcmp(levels[1], "level") == 0)
			slctlthr_param_log_level(fd, scmh, sp, levels, nlevels);
		else
			goto invalid;
	} else
		goto invalid;
	return;

 invalid:
	while (nlevels > 1)
		levels[--nlevels][-1] = '.';
	slctlthr_senderrmsg(fd, scmh,
	    "invalid field/value: %s", sp->sp_field);
}

#if 0
/*
 * slctlthr_sendrep_iostat - send a response to a "getiostat" inquiry.
 * @fd: client socket descriptor.
 * @scmh: already filled-in slash control message header.
 * @sist: iostat message structure to be filled in and sent out.
 */
void
slctlthr_sendrep_iostat(int fd, struct slctlmsghdr *scmh,
    struct slctlmsg_iostats *sist)
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
			slctlthr_sendmsgv(fd, scmh, sist);

			if (strlen(ist->ist_name) == strlen(name))
				break;
		}
	freelock(&iostatsListLock);

	if (!found && !all)
		slctlthr_senderrmsg(fd, scmh,
		    "unknown iostats: %s", name);
}
#endif

/*
 * slctlthr_procmsg - process a message from a client.
 * @fd: client socket descriptor.
 * @scmh: slash control message header from client.
 * @scm: contents of slash control message from client.
 *
 * Notes: the length of the data buffer `scm' has not yet
 * been verified for each message type case and must be
 * checked in each case before it can be dereferenced,
 * since there is no way to know until this point.
 */
void
slctlthr_procmsg(int fd, struct slctlmsghdr *scmh, void *scm)
{
	struct psc_thread **threads;
	struct slctlmsg_hashtable *sht;
	struct slctlmsg_loglevel *sll;
	struct slctlmsg_stats *sst;
	struct slctlmsg_param *sp;
	struct slctlmsg_lc *slc;
	int n, nthr;

	/* XXX lock or snapshot nthreads so it doesn't change underneath us */
	nthr = dynarray_len(&pscThreads);
	threads = dynarray_get(&pscThreads);
	switch (scmh->scmh_type) {
	case SCMT_GETSUBSYS:
		slctlthr_sendrep_getsubsys(fd, scmh);
		break;
	case SCMT_GETLOGLEVEL:
		sll = scm;
		if (scmh->scmh_size != sizeof(*sll))
			goto badlen;
		if (strcasecmp(sll->sll_thrname,
		    STHRNAME_EVERYONE) == 0) {
			for (n = 0; n < nthr; n++)
				slctlthr_sendrep_getloglevel(fd,
				    scmh, sll, threads[n]);
		} else {
			for (n = 0; n < nthr; n++)
				if (strcasecmp(sll->sll_thrname,
				    threads[n]->pscthr_name) == 0) {
					slctlthr_sendrep_getloglevel(fd,
					    scmh, sll, threads[n]);
					break;
				}
			if (n == nthr)
				slctlthr_senderrmsg(fd, scmh,
				    "unknown thread: %s",
				    sll->sll_thrname);
		}
		break;
	case SCMT_GETHASHTABLE: {
		sht = scm;
		if (scmh->scmh_size != sizeof(*sht))
			goto badlen;
		slctlthr_sendreps_gethashtable(fd, scmh, sht);
		break;
	    }
	case SCMT_GETSTATS:
		sst = scm;
		if (scmh->scmh_size != sizeof(*sst))
			goto badlen;
		if (strcasecmp(sst->sst_thrname,
		    STHRNAME_EVERYONE) == 0) {
			for (n = 0; n < nthr; n++)
				slctlthr_sendrep_getstats(fd,
				    scmh, sst, threads[n], 1);
		} else {
			for (n = 0; n < nthr; n++)
				if (strcasecmp(sst->sst_thrname,
				    threads[n]->pscthr_name) == 0) {
					slctlthr_sendrep_getstats(fd,
					    scmh, sst, threads[n], 0);
					break;
				}
			if (n == nthr)
				slctlthr_senderrmsg(fd, scmh,
				    "unknown thread: %s",
				    sst->sst_thrname);
		}
		break;
	case SCMT_GETLC:
		slc = scm;
		if (scmh->scmh_size != sizeof(*slc))
			goto badlen;
		if (strcmp(slc->slc_name, SLC_NAME_ALL) == 0) {
			list_cache_t *lc;

			spinlock(&pscListCachesLock);
			psclist_for_each_entry(lc, &pscListCaches, lc_index_lentry) {
				LIST_CACHE_LOCK(lc);
				slctlthr_sendrep_getlc(fd, scmh, slc, lc);
			}
			freelock(&pscListCachesLock);
		} else
			slctlthr_sendrep_getlc(fd, scmh, slc,
			    lc_lookup(slc->slc_name));
		break;
	case SCMT_GETPARAM:
	case SCMT_SETPARAM:
		sp = scm;
		if (scmh->scmh_size != sizeof(*sp))
			goto badlen;
		slctlthr_sendreps_param(fd, scmh, sp);
		break;
#if 0
	case SCMT_GETIOSTAT:
		sist = scm;
		if (scmh->scmh_size != sizeof(*sist))
			goto badlen;
		slctlthr_sendrep_iostat(fd, scmh, sist);
		break;
#endif
	default:
		warnx("unexpected msg type; type=%d size=%zu",
		    scmh->scmh_type, scmh->scmh_size);
		break;
	}
	return;
 badlen:
	warnx("unexpected msg size; type=%d, siz=%zu", scmh->scmh_type,
	    scmh->scmh_size);
}

/*
 * slctlthr_service - satisfy a client connection.
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
 * slash operation are being processed.
 *
 * Disadvantages: if we don't go to sleep during processing of client
 * connection, anyone can denial the slash service quite easily.
 */
void
slctlthr_service(int fd)
{
	struct slctlmsghdr scmh;
	size_t scmsiz;
	void *scm;
	ssize_t n;

	scm = NULL;
	scmsiz = 0;
	while ((n = read(fd, &scmh, sizeof(scmh))) != -1 && n != 0) {
		if (n != sizeof(scmh)) {
			psc_notice("short read on slctlmsghdr; read=%zd", n);
			continue;
		}
		if (scmh.scmh_size == 0) {
			psc_warnx("empty slctlmsg; type=%d", scmh.scmh_type);
			continue;
		}
		if (scmh.scmh_size > scmsiz) {
			scmsiz = scmh.scmh_size;
			if ((scm = realloc(scm, scmsiz)) == NULL)
				err(1, "realloc");
		}
		n = read(fd, scm, scmh.scmh_size);
		if (n == -1)
			err(1, "read");
		if ((size_t)n != scmh.scmh_size) {
			psc_warn("short read on slctlmsg contents; "
			    "read=%zu; expected=%zu",
			    n, scmh.scmh_size);
			break;
		}
		slctlthr_procmsg(fd, &scmh, scm);
		sched_yield();
	}
	if (n == -1)
		err(1, "read");
	free(scm);
}

/*
 * slctlthr_main - main slash control thread client-servicing loop.
 * @fn: path to control socket.
 */
__dead void
slctlthr_main(const char *fn)
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
		slctlthr(&slashControlThread)->sc_st_nclients++;
		slctlthr_service(fd);
		close(fd);
	}
	/* NOTREACHED */
}

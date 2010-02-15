/* $Id$ */

#ifndef _FIDC_IOD_H_
#define _FIDC_IOD_H_

struct fidc_membh;

struct fcoo_iod_info {
	int			fii_fd;		/* open file descriptor */
};

#define fcoo_2_fii(fcoo)	((struct fcoo_iod_info *)(fcoo)->fcoo_pri)
#define fcmh_2_fd(fcmh)		fcoo_2_fii((fcmh)->fcmh_fcoo)->fii_fd

int fcmh_ensure_has_fii(struct fidc_membh *);

#endif /* _FIDC_IOD_H_ */

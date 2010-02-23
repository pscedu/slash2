/* $Id$ */

#ifndef _FIDC_IOD_H_
#define _FIDC_IOD_H_

struct fidc_membh;

struct fcoo_iod_info {
	int			fii_fd;		/* open file descriptor */
};

#define fcoo_2_fii(fcoo)	((struct fcoo_iod_info *)fcoo_get_pri(fcoo))
#define fcmh_2_fd(fcmh)		fcoo_2_fii((fcmh)->fcmh_fcoo)->fii_fd

int fcmh_load_fcoo(struct fidc_membh *);

#endif /* _FIDC_IOD_H_ */

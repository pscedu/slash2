/* $Id$ */

#ifndef _SLASHD_H_
#define _SLASHD_H_

#define cfd_2_mexpfcm(cfd)	((struct mexpfcm *)(cfd)->pri)
#define cfd_2_fcmh(cfd)		cfd_2_mexpfcm(cfd)->mexpfcm_fcmh
#define cfd_2_fmdsi(cfd)	fcmh_2_fmdsi(cfd_2_fcmh(cfd))
#define cfd_2_zfsdata(cfd)	fcmh_2_zfsdata(cfd_2_fcmh(cfd))

extern struct cfdops		mdsCfdOps;
extern struct slash_creds	rootcreds;
extern int			allow_internal_fsaccess;

#endif /* _SLASHD_H_ */

Overview
To run an instance of SLASH2, you need to configure three types of services: metadata service (slashd), I/O service (sliod), and I/O client service (mount_slash). There are manual pages for each of these three types of services and all the utilities involved. The best place to start is as follows:
$ man sladm
Basically, the above man pages outlines three major steps to configure a slash2 instance:
On the machine where the MDS daemon runs, we need to create a ZFS pool with the help of zfs-fuse. The pool has to be formatted with slmkfs utility before use. In addition, we must create a journal file for the MDS daemon to track on-going operations.
On a machine where a SLIOD daemon runs, we need to use slmkfs utility to format the directory tree to be used as the back store for the SLIOD daemon.
On a machine where a mount_slash daemon runs, we only need to run the mount_slash binary.
Starting the metadata server
The slashd manpage slashd(8) details the different command line parameters. Depending on your configuration many of the default parameters should suffice. Here's a command line used for running SLASH2 based on the the example configuration (assuming that the example configuration has been stored at /etc/s2_example.conf"
# slashd -f /etc/s2_example.conf &
A more sophisticated example of a run time script may be found in the SLASH2 distribution. Note this script should be modified for your own environment.
projects/apps/arc/slashd.sh
Determining whether the MDS is running
The command slmctl(8) serves as the interface to the running slashd process. The man page details the numerous options available to the administrator for querying and changing internal parameters. A simple test to see if your MDS is running would be the following:
$ slmctl -sc
resource                                     host type     flags stvrs txcr #ref
================================================================================
XWFS
  xwfs_psc_ios7_0                        sense7.psc.edu serial   -OM-- 19149    8    0
  xwfs_tacc_0                         129.114.32.90 serial   -OM-- 19150    8    0
  xwfs_tacc_1                         129.114.32.91 serial   -OM-- 19150    8    0
  xwfs_tacc_2                         129.114.32.92 serial   -OM-- 19150    8    0
  xwfs_tacc_3                         129.114.32.93 serial   --M-- 19150    8    0
clients
		       firehose2.psc.teragrid.org          -O---     0    8    2

The above command shows lists the clients and I/O servers connected to the MDS. The fact that the command returned this output shows the MDS is operational.
Using the zfs and zpool commands
Once your MDS is operational, the zfs and zpool commands will be available for use towards ZFS administration.
Note: these commands were taken on a test MDS here at PSC and do not reflect the example configuration.
$ zfs get compressratio
NAME            PROPERTY       VALUE  SOURCE
xwfs_test       compressratio  3.28x  -
xwfs_test@test  compressratio  3.04x  -
$ zpool iostat
	       capacity     operations    bandwidth
pool        alloc   free   read  write   read  write
----------  -----  -----  -----  -----  -----  -----
xwfs_test   1001M   148G      3      0  7.69K  7.23K
$ zpool scrub xwfs_test
$ zfs send xwfs_test@test > /local/xwfs_test@test.zstream
Running the SLASH2 I/O service (sliod)
Prior to starting sliod on the I/O servers, make sure the following items are accessible on the I/O servers:
Contents of /var/lib/slash from the MDS - basically just make a copy of this directory.
The configuration file has been copied to the I/O server. At this time each server and client must maintain a copy of the configuration.
Once the I/O server has those items, and the slmkfs has been run, sliod may be started. Defer to the sliod manual for the list of options. The following assumes the slash2 configuration file has been placed at /etc/s2_example.conf:
# sliod -f /etc/s2_example.conf &
Verifying sliod with slictl
Similar to slmctl, the sliod has its own control interface program called slictl(8). To ensure that sliod is running and has initiated contact with the MDS run the following command:
$ slictl -sc
resource                                    host type      flags stvrs txcr #ref
================================================================================
XWFS
  xwfs_mds                         citron.psc.edu mds      -O--P 19111    8    0
  xwfs_tacc_0                         129.114.32.90 serial   -----     0    0    0
  xwfs_tacc_1                         129.114.32.91 serial   -----     0    0    0
  xwfs_tacc_2                         129.114.32.92 serial   -----     0    0    0
  xwfs_tacc_3                         129.114.32.93 serial   -----     0    0    0
Running the Client - mount_slash
The client binary is named mount_slash(8). To run the client one must have done the following:
Copied the contents of /var/lib/slash from the metadata server to the client
Placed a copy of the configuration file in the client's local file system.
Created the file system mount point (i.e. 'mkdir /S2')
Within the context of this example, the following command line will launch the client. Please refer to the mount_slash man page for more details.
# mount_slash -f /etc/s2_example.conf -U /S2 &
At this time your mount point should be fully functional and ready for use.
Verifying the client with msctl
This shows the client being connected to the MDS and several I/O servers.
$ msctl -sc
resource                                    host type      flags stvrs txcr #ref
================================================================================
XWFS
  xwfs_mds                         citron.psc.edu mds      -O--- 19111    8  601
  xwfs_psc_ios7_0                        sense7.psc.edu serial   ----- 19149    8    0
  xwfs_tacc_0                         129.114.32.90 serial   -O--- 19150    8    0
  xwfs_tacc_1                         129.114.32.91 serial   -O--- 19150    8    0
  xwfs_tacc_2                         129.114.32.92 serial   -O--- 19150    8    0
  xwfs_tacc_3                         129.114.32.93 serial   -O--- 19150    8    0

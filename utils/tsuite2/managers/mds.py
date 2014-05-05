from managers import sl2gen
from utils.ssh import SSH
from paramiko import SSHException

import sys
import logging
log = logging.getLogger("sl2.mds")

def create_mds(tsuite):
  """Initialize MDS resources for testing.

    Args:
      tsuite: tsuite runtime."""

  #Create the MDS systems
  for mds in tsuite.sl2objects["mds"]:

    #Create monolithic reference/replace dict
    repl_dict = dict(tsuite.src_dirs, **tsuite.build_dirs)
    repl_dict = dict(repl_dict, **mds)

    #Create remote connection to server
    try:
      #Can probably avoid doing user, host everytime
      user, host = tsuite.user, mds["host"]
      log.debug("Connecting to {0}@{1}".format(user, host))
      ssh = SSH(user, host, '')

      cmd = """
      $SHELL -c "cd {src} && make printvar-CC >/dev/null"
      pkill zfs-fuse || true
      $SHELL -c "{zfs_fuse} &"
      sleep 2
      {zpool} destroy {zpool_name} || true
      sleep 2
      {zpool} create -m {zpool_path} -f {zpool_name} {zpool_args}
      sleep 2
      {zpool} set cachefile={zpool_cache} {zpool_name}
      sleep 2
      {slmkfs} -u {fsuuid} -I {site_id} {zpool_path}
      sleep 2
      sync
      umount {zpool_path}
      pkill zfs-fuse || true
      sleep 2
      $SHELL -c "{zfs_fuse} &"
      sleep 2
      {zpool} import {zpool_name} || true
      sleep 2
      pkill zfs-fuse || true
      sleep 2
      mkdir -p {datadir}
      {slmkjrnl} -D {datadir} -b {jrnldev} -f -u {fsuuid}""".format(**repl_dict)

      screen_name = "ts.mds."+mds["id"]

      sl2gen.sl_screen_and_wait(tsuite, ssh, cmd, screen_name)

      log.info("Finished creating {0}".format(mds["name"]))
      ssh.close()

    except SSHException, e:
      log.fatal("Error with remote connection to {0} with res {1}!"\
          .format(mds["host"], mds["name"]))
      tsuite.shutdown()


def launch_mds(tsuite):
  """Launch MDS/slashd daemons."""

  gdbcmd_path = tsuite.conf["slash2"]["mds_gdb"]
  sl2gen.launch_gdb_sl(tsuite, "mds", tsuite.sl2objects["mds"], "slashd", gdbcmd_path)

def kill_mds(tsuite):
  """Kill MDS/slashd daemons."""
  sl2gen.stop_slash2_socks(tsuite, "mds", tsuite.sl2objects["mds"], "slmctl", "slashd")

from managers import sl2gen
from utils.ssh import SSH

from paramiko import SSHException

import sys

import logging
log = logging.getLogger("sl2.mnt")

def launch_mnt(tsuite):
  """Launch mount slash."""

  gdbcmd_path = tsuite.conf["slash2"]["mnt_gdb"]
  sl2gen.launch_gdb_sl(tsuite, "client", tsuite.sl2objects["client"], "mount_slash", gdbcmd_path)

def kill_mnt(tsuite):
  """Kill ION daemons.

  Args:
    tsuite: runtime tsuite."""
  
  for client in tsuite.sl2objects["client"]:
    ssh = SSH(tsuite.user, client["host"])
    if not ssh.run("sudo umount {0}".format(tsuite.build_dirs["mp"]))["err"] == []:
      log.critical("Cannot unmount client mountpoint at {0} @ {1}.".format(tsuite.build_dirs["mp"], client["host"]))

  sl2gen.stop_slash2_socks(tsuite, "client", tsuite.sl2objects["client"], "msctl", "mount_slash")

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
  sl2gen.stop_slash2_socks(tsuite, "client", tsuite.sl2objects["client"], "msctl")

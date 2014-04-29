from managers import sl2gen
from utils.ssh import SSH
from paramiko import SSHException

import sys
import logging
log = logging.getLogger("sl2.ion")

def launch_ion(tsuite):
  """Launch ION daemons.

  Args:
    tsuite: tsuite runtime."""

  gdbcmd_path = tsuite.conf["slash2"]["ion_gdb"]
  sl2gen.launch_gdb_sl(tsuite, "ion", tsuite.sl2objects["ion"], "sliod", gdbcmd_path)

def create_ion(tsuite):
  """Create ION file systems.

  Args:
    tsuite: tsuite runtime."""

  for ion in tsuite.sl2objects["ion"]:

    #Create monolithic reference/replace dict
    repl_dict = dict(tsuite.src_dirs, **tsuite.build_dirs)
    repl_dict = dict(repl_dict, **ion)

    #Create remote connection to server
    try:
      user, host = tsuite.user, ion["host"]
      log.debug("Connecting to {0}@{1}".format(user, host))
      ssh = SSH(user, host, '')

      cmd = """
      mkdir -p {datadir}
      mkdir -p {fsroot}
      {slmkfs} -Wi -u {fsuuid} -I {site_id} {fsroot}"""\
      .format(**repl_dict)

      sock_name = "ts.ion."+ion["id"]

      sl2gen.sl_screen_and_wait(tsuite, ssh, cmd, sock_name)

      log.info("Finished creating {0}!".format(ion["name"]))
      ssh.close()

    except SSHException, e:
      log.fatal("Error with remote connection to {0} with res {1}!"\
          .format(ion["host"], ion["name"]))
      tsuite.shutdown()

def kill_ion(tsuite):
  """Kill ION daemons.

  Args:
    tsuite: runtime tsuite."""
  sl2gen.stop_slash2_socks(tsuite, "ion", tsuite.sl2objects["ion"], "slictl", "sliod")

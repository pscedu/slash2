import time, logging, sys
from os import path

from utils.ssh import SSH
from tsuite import *

log = logging.getLogger("sl2.gen")

def repl(lookup, string):
  """Replaces keywords within a string.

  Args:
    lookup: dict in which to look up keywords.
    string: string with embedded keywords. Ex. %key%.
  Return:
    String containing the replacements."""

  return re.sub("%([\w_]+)%",
    #If it is not in the lookup, leave it alone
    lambda m: lookup[m.group(1)]
      if
        m.group(1) in lookup
      else
        "%{0}%".format(m.group(1)),
    string)

def repl_file(lookup, text):
  """Reads a file and returns an array with keywords replaced.

  Args:
    lookup: dict in which to look up keywords.
    text: file containing strings with embedded keywords. Ex. %key%.
  Return:
    String containing all replacements. Returns none if not able to parse."""

  try:
    with open(text, "r") as f:
     return "".join([repl(lookup, line) for line in f.readlines()])
  except IOError, e:
    return None

def sl_screen_and_wait(tsuite, ssh, cmd, screen_name):
  """Common slash2 screen functionality.
  Check for existing sock, run the cmd, and wait to see if it timed out or executed successfully.

  Args:
    tsuite: tsuite runtime.
    ssh: remote server connection.
    cmd: command to run remotely
    screen_name: name of screen sock to wait for."""

  #Run command string in screen
  if not ssh.run_screen(cmd, screen_name, tsuite.conf["slash2"]["timeout"]):
    log.fatal("Screen session {0} already exists in some form! Attach and deal with it.".format(screen_name))
    tsuite.shutdown()

  wait = ssh.wait_for_screen(screen_name)

  if wait["timedout"]:
    log.critical("{0} timed out! screen -r {0}-timed and check it out."\
        .format(screen_name))
    tsuite.shutdown()
  elif wait["errored"]:
    log.critical("{0} exited with a nonzero return code. screen -r {0}-error and check it out."\
        .format(screen_name))
    tsuite.shutdown()

def handle_authbuf(tsuite, ssh, res_type):
  """Deals with the transfer of authbuf keys. Returns True if the authbuf key needs
      to be pulled after lauching this object

  Args:
    tsuite: tsuite runtime.
    ssh: remote server connection
    res_type: slash2 resource type."""

  if not hasattr(tsuite, "authbuf_obtained"):
    tsuite.authbuf_obtained = False

  if res_type == "mds" and not tsuite.authbuf_obtained:
    log.debug("First MDS found at {0}; Copying authbuf key after launch".format(ssh.host))
    return True
  else:
    assert(tsuite.authbuf_obtained != False)
    log.debug("Authbuf key already obtained. Copying to {0}".format(ssh.host))
    location = path.join(tsuite.build_dirs["datadir"], "authbuf.key")
    try:
      os.system("sudo chmod 0666 {0}".format(location))
      ssh.copy_file(location, location)
      os.system("sudo chmod 0400 {0}".format(location))
      ssh.run("sudo chmod 0400 {0}".format(location))
    except IOError:
      log.critical("Failed copying authbuf key to {0}".format(ssh.host))
      tsuite.shutdown()

    return False

def pull_authbuf(tsuite, ssh):
  """Pulls the authbuf key from the remote connection and stores it locally

  Args:
    ssh: remote server connection """

  location = path.join(tsuite.build_dirs["datadir"], "authbuf.key")
  assert(not tsuite.authbuf_obtained)

  try:
    ssh.run("sudo chmod 666 {0}".format(location))
    ssh.pull_file(location, location)
    ssh.run("sudo chmod 400 {0}".format(location))
    tsuite.authbuf_obtained = True
  except IOError:
    log.critical("Failed pulling the authbuf key from {0}.".format(ssh.host))
    tsuite.shutdown()

def launch_gdb_sl(tsuite, sock_name, sl2objects, res_bin_type, gdbcmd_path):
  """Generic slash2 launch service in screen+gdb. Will also copy over authbuf keys.

  Args:
    tsuite: tsuite runtime.
    sock_name: name of sl2 sock.
    sl2objects: list of objects to be launched.
    res_bin_type: key to bin path in src_dirs.
    gdbcmd_path: path to gdbcmd file."""

  #res_bin_type NEEDS to be a path in src_dirs
  assert(res_bin_type in tsuite.src_dirs)

  for sl2object in sl2objects:
    log.debug("Initializing environment > {0} @ {1}".format(sl2object["name"], sl2object["host"]))

    #Remote connection
    user, host = tsuite.user, sl2object["host"]
    log.debug("Connecting to {0}@{1}".format(user, host))
    ssh = SSH(user, host, '')

    #Acquire and deploy authbuf key
    need_authbuf = handle_authbuf(tsuite, ssh, sl2object["type"])

    ls_cmd = "ls {0}/".format(tsuite.build_dirs["ctl"])
    result = ssh.run(ls_cmd)

    present_socks = [res_bin_type in sock for sock in result["out"]].count(True)

    #Create monolithic reference/replace dict
    repl_dict = dict(tsuite.src_dirs, **tsuite.build_dirs)
    repl_dict = dict(repl_dict, **sl2object)

    if "id" not in sl2object.keys():
      sl2object["id"] = 0

    #Create gdbcmd from template
    gdbcmd_build_path = path.join(tsuite.build_dirs["base"],
        "{0}_{1}".format(sl2object["id"], path.basename(gdbcmd_path)))

    new_gdbcmd = repl_file(repl_dict, gdbcmd_path)

    if new_gdbcmd:
      with open(gdbcmd_build_path, "w") as f:
        f.write(new_gdbcmd)
        f.close()
        log.debug("Wrote gdb cmd to {0}".format(gdbcmd_build_path))
        log.debug("Remote copying gdbcmd.")
        ssh.copy_file(gdbcmd_build_path, gdbcmd_build_path)
    else:
      log.fatal("Unable to parse gdb cmd at {0}!".format(gdbcmd_path))
      tsuite.shutdown()

    cmd = "sudo gdb -f -x {0} {1}".format(gdbcmd_build_path, tsuite.src_dirs[res_bin_type])
    screen_sock_name = "sl.{0}.{1}".format(sock_name, sl2object["id"])

    #Launch slash2 in gdb within a screen session
    ssh.run_screen(cmd, screen_sock_name)

    #Wait two seconds to make sure slash2 launched without errors
    time.sleep(2)

    screen_socks = ssh.list_screen_socks()
    if screen_sock_name + "-error" in screen_socks or screen_sock_name not in screen_socks:
      log.fatal("sl2object {0}:{1} launched with an error. Resume to {2} and resolve it."\
          .format(sl2object["name"], sl2object["id"], screen_sock_name+"-error"))
      tsuite.shutdown(ignore=sock_name)

    log.debug("Waiting for {0} sock on {1} to appear.".format(sock_name, host))
    count = 0
    while True:
      result = ssh.run(ls_cmd, quiet=True)
      if not all(res_bin_type not in sock for sock in result["out"]):
        break
      time.sleep(1)
      count += 1
      if count == int(tsuite.conf["slash2"]["timeout"]):
        log.fatal("Cannot find {0} sock on {1}. Resume to {2} and resolve it. "\
          .format(res_bin_type, sl2object["id"], screen_sock_name))
        tsuite.shutdown(ignore=sock_name)

    #grab pid for resouce querying later
    #TODO: do not grab other running instances
    sl2object["pid"] = ssh.run("pgrep {0}".format(res_bin_type))['out'][0].strip()
    log.debug("Found {0} pid on {1} : {2}".format(res_bin_type, host, sl2object["pid"]))

    if need_authbuf:
      pull_authbuf(tsuite, ssh)

    ssh.close()

def stop_slash2_socks(tsuite, sock_name, sl2objects, ctl_type, daemon_type):
  """ Terminates all slash2 socks and screen socks on a generic host.
  Args:
    tsuite: tsuite runtime.
    sock_name: name of sl2 sock.
    sl2objects: list of objects to be launched.
    ctl_type: key to ctl path in src_dirs
    daemon_type: key to daemon path in src_dirs"""

  assert(ctl_type in tsuite.src_dirs)
  assert(daemon_type in tsuite.src_dirs)

  for sl2object in sl2objects:
    log.info("Killing {0} @ {1}".format(sl2object["name"], sl2object["host"]))

    #Remote connection
    user, host = tsuite.user, sl2object["host"]
    log.debug("Connecting to {0}@{1}".format(user, host))

    ssh = None
    try:
      ssh = SSH(user, host, '')
    except Exception:
      log.error("Unable to connect to {0}@{1}".format(user, host))
      return

    cmd = "{0} -S {1}/{2}.{3}.sock stop".format(tsuite.src_dirs[ctl_type], tsuite.build_dirs["ctl"], daemon_type, host)
    ssh.run(cmd)

    if "id" not in sl2object.keys():
      sl2object["id"] = 0

    screen_sock_name = "sl.{0}.{1}".format(sock_name, sl2object["id"])
    ssh.kill_screens(screen_sock_name, exact_sock=True)

    ssh.close()

def store_build_time(tsuite, res, host, build_time):
  """Handle storing the build time for processing.

  Args:
    tsuite: runtime tsuite instance
    res: resource listing type
    host: host of the process
    build_time: time in seconds required to create"""

  if res not in tsuite.test_report:
    tsuite.test_report["build"][res] = {}

  tsuite.test_report["build"][res][host] = {
      "build_time": build_time
  }


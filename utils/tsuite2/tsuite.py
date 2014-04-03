import logging, re, os, sys
import glob, time

from random import randrange
from os import path
from paramiko import SSHException

from sl2 import SL2Res
from ssh import SSH

log = logging.getLogger("slash2")

class TSuite(object):
  """SLASH2 File System Test Suite."""

  def __init__(self, conf):
    """Initialization of the TSuite.

    Args:
      conf: configuration dict from configparser."""

    #Test suite directories
    #Relative paths, replaced in init
    self.build_dirs = {
      # "base" populated in init
      "mp"   : "%base%/mp",
      "datadir": "%base%/data",
      "ctl"  : "%base%/ctl",
      "fs"   : "%base%/fs"
    }

    self.authbuf_key = None

    self.src_dirs = {
      # "src" populated in init
      "slbase"  : "%src%/slash_nara",
      "tsbase"  : "%slbase%/../tsuite",
      "zpool"   : "%src%/zfs/src/cmd/zpool/zpool",
      "zfs_fuse": "%slbase%/utils/zfs-fuse.sh",
      "sliod"   : "%slbase%/sliod/sliod",
      "slmkjrnl": "%slbase%/slmkjrnl/slmkjrnl",
      "slmctl"  : "%slbase%/slmctl/slmctl",
      "slictl"  : "%slbase%/slictl/slictl",
      "slashd"  : "%slbase%/slashd/slashd",
      "slkeymgt": "%slbase%/slkeymgt/slkeymgt",
      "slmkfs"  : "%slbase%/slmkfs/slmkfs"
    }

    self.tsid = None
    self.rootdir = None

    self.sl2objects = {}
    self.conf = conf
    self.clients = {}

    self.user = os.getenv("USER")

    #TODO: Rename rootdir in src_dir fashion
    self.rootdir = self.conf["tsuite"]["rootdir"]
    self.src_dirs["src"] = self.conf["source"]["srcroot"]

    #Necessary to compute relative paths
    self.build_base_dir()
    log.debug("Base directory: {0}".format(self.build_dirs["base"]))

    self.replace_rel_dirs(self.build_dirs)

    if not self.mk_dirs(self.build_dirs.values()):
      log.fatal("Unable to create some necessary directory!")
      sys.exit(1)
    log.info("Successfully created build directories")

    #Compute relative paths for source dirs
    self.replace_rel_dirs(self.src_dirs)

    #Also check for embedded build paths
    self.replace_rel_dirs(self.src_dirs, self.build_dirs)

    if not self.parse_slash2_conf():
      log.critical("Error parsing slash2 configuration file!")
      sys.exit(1)

    log.info("slash2 configuration parsed successfully.")

    #Show the resources parsed
    objs_disp = [
      "{0}:{1}".format(res, len(res_list))\
          for res, res_list in self.sl2objects.items()
    ]
    log.debug("Found: {0}".format(", ".join(objs_disp)))

    for sl2_obj in self.all_objects():
      ssh = SSH(self.user, sl2_obj["host"], '')
      log.debug("Creating build directories on {0}@{1}".format(sl2_obj["name"], sl2_obj["host"]))
      for d in self.build_dirs.values():
        ssh.make_dirs(d)
      ssh.close()

  def all_objects(self):
    """Returns all sl2objects in a list."""
    objects = []
    for res, res_list in self.sl2objects.items():
      objects.extend(res_list)
    return objects

  def check_status(self):
    """Generate general status report for all sl2 objects.

    Returns: {
      "type":[ {"host": ..., "reports": ... } ],
      ...
    }"""
    report = {}

    #Operations based on type
    ops = {
        "all": {
          "load": "cat /proc/loadavg | cut -d' ' -f1,2,3",
          "mem_total": "cat /proc/meminfo | head -n1",
          "mem_free": "sed -n 2,2p /proc/meminfo",
          "uptime": "cat /proc/uptime | head -d' ' -f1",
          "disk_stats": "df -hl"
        },
        "mds": {
          "connections":"{slmctl} -sconnections",
          "iostats":"{slmctl} -siostats"
        },
        "ion": {
          "connections": "{slictl} -sconnections",
          "iostats": "{slictl} -siostats"
        }
    }

    for sl2_restype in self.sl2objects.keys():

      report[sl2_restype] = []

      obj_ops = ops["all"]
      if sl2_obj[sl2_restype] in ops:
        obj_ops = dict(ops["all"].items() + ops[sl2_restype].items())

      for sl2_obj in self.sl2objects[sl2_restype]:
        obj_report = {
          "host": sl2_obj["host"],
          "id": sl2_obj["id"],
          "reports": {}
        }
        user, host = self.user, sl2_obj["host"]
        log.debug("Connecting to {0}@{1}".format(user, host))
        ssh = SSH(user, host, '')
        for op, cmd in obj_ops.items():
          obj_report["reports"][op] = ssh.run(cmd, timeout=2)

        report[sl2_restype].append(obj_report)
        log.debug("Status check completed for {0} [{1}]".format(host, sl2_restype))
        ssh.close()
    return report

  def build_mds(self):
    """Initialize MDS resources for testing."""

    #Create the MDS systems
    for mds in self.sl2objects["mds"]:

      #Create monolithic reference/replace dict
      repl_dict = dict(self.src_dirs, **self.build_dirs)
      repl_dict = dict(repl_dict, **mds)

      #Create remote connection to server
      try:
        #Can probably avoid doing user, host everytime
        user, host = self.user, mds["host"]
        log.debug("Connecting to {0}@{1}".format(user, host))
        ssh = SSH(user, host, '')

        cmd = """
        $SHELL -c "cd {src} && make printvar-CC >/dev/null"
        pkill zfs-fuse || true
        $SHELL -c "{zfs_fuse} &"
        sleep 2
        {zpool} destroy {zpool_name} || true
        {zpool} create -m {zpool_path} -f {zpool_name} {zpool_args}
        {zpool} set cachefile={zpool_cache} {zpool_name}
        {slmkfs} -u {fsuuid} -I {site_id} {zpool_path}
        sync
        sync
        umount {zpool_path}
        pkill zfs-fuse
        mkdir -p {datadir}
        {slmkjrnl} -D {datadir} -u {fsuuid} -f""".format(**repl_dict)

        screen_name = "ts.mds."+mds["id"]

        self.__sl_screen_and_wait(ssh, cmd, screen_name)

        log.info("Finished creating {0}".format(mds["name"]))
        ssh.close()

      except SSHException, e:
        log.fatal("Error with remote connection to {0} with res {1}!"\
            .format(mds["host"], mds["name"]))
        sys.exit(1)

  def run_tests(self):
    """Uploads and runs each test on each client."""
    test_dir = self.conf["tests"]["testdir"]
    ssh_clients = [SSH(self.user, host) for host in self.clients]
    map(lambda ssh: ssh.make_dirs("modules"), ssh_clients)
    for test in os.listdir(test_dir):
      if test.endswith(".py"):
        test_path = path.join(test_dir, test)
        log.debug("Found {0} test".format(test))
        map(lambda ssh: ssh.copy_file(test_path, path.join("modules", test))

  def build_ion(self):
    """Create ION file systems."""

    for ion in self.sl2objects["ion"]:

      #Create monolithic reference/replace dict
      repl_dict = dict(self.src_dirs, **self.build_dirs)
      repl_dict = dict(repl_dict, **ion)

      #Create remote connection to server
      try:
        user, host = self.user, ion["host"]
        log.debug("Connecting to {0}@{1}".format(user, host))
        ssh = SSH(user, host, '')

        cmd = """
        mkdir -p {datadir}
        mkdir -p {fsroot}
        {slmkfs} -Wi -u {fsuuid} -I {site_id} {fsroot}"""\
        .format(**repl_dict)

        sock_name = "ts.ion."+ion["id"]

        self.__sl_screen_and_wait(ssh, cmd, sock_name)

        log.info("Finished creating {0}!".format(ion["name"]))
        ssh.close()

      except SSHException, e:
        log.fatal("Error with remote connection to {0} with res {1}!"\
            .format(ion["host"], ion["name"]))
        sys.exit(1)

  def launch_mnt(self):
    """Launch mount slash."""

  def launch_ion(self):
    """Launch ION daemons."""

    gdbcmd_path = self.conf["slash2"]["ion_gdb"]
    self.__launch_gdb_sl("ion", self.sl2objects["ion"], "sliod", gdbcmd_path)

  def launch_mds(self):
    """Launch MDS/slashd daemons."""

    gdbcmd_path = self.conf["slash2"]["mds_gdb"]
    self.__launch_gdb_sl("slashd", self.sl2objects["mds"], "slashd", gdbcmd_path)

  def kill_mds(self):
    """Kill MDS/slashd daemons."""
    self.__stop_slash2_socks("slashd", self.sl2objects["mds"], "slmctl")

  def kill_ion(self):
    """Kill ION daemons."""
    self.__stop_slash2_socks("sliod", self.sl2objects["ion"], "slictl")

  def __get_authbuf(self, ssh):
    """Trys to acquire the authbuf key from the data dir. Will cache it for subsequent calls.

    Args:
      ssh: ssh connection to get/transfer authbuf to."""

    authbuf = path.join(self.build_dirs["base"], "authbuf.key")

    if not self.authbuf_key:
      sh = """if [[ -a {0} ]]; then; cat {0}; fi;""".format(authbuf)
      result = ssh.run(sh)
      if result["exit"] == 0:
        #Hacky way of converting it to a safe hex string
        self.authbuf_key = "\\x" + "\\x".join([c.encode("hex") for c in "".join(result["out"])])
        log.debug("Found authbuf key.")
      else: return
    sh = 'echo "{0}" > {1}'.format(self.authbuf_key, authbuf)
    result = ssh.run(sh)
    log.debug("Written authbuf key successfully!" if result["error"] == 0 else "Failed to write authbuf key")

  def __launch_gdb_sl(self, sock_name, sl2objects, res_bin_type, gdbcmd_path):
    """Generic slash2 launch service in screen+gdb. Will also copy over authbuf keys.

    Args:
      sock_name: name of sl2 sock.
      sl2objects: list of objects to be launched.
      res_bin_type: key to bin path in src_dirs.
      gdbcmd_path: path to gdbcmd file."""

    #res_bin_type NEEDS to be a path in src_dirs
    assert(res_bin_type in self.src_dirs)

    for sl2object in sl2objects:
      log.debug("Initializing environment > {0} @ {1}".format(sl2object["name"], sl2object["host"]))

      #Remote connection
      user, host = self.user, sl2object["host"]
      log.debug("Connecting to {0}@{1}".format(user, host))
      ssh = SSH(user, host, '')

      #Acquire and deploy authbuf key
      self.__get_authbuf(ssh)


      ls_cmd = "ls {0}/{1}*.sock".format(self.build_dirs["ctl"], sock_name)
      result = ssh.run(ls_cmd)

      if len(result['err']) > 0:
        present_socks = 0
      else:
        present_socks = len(result['out'])

      if present_socks >= 1:
        log.warning("There are already {0} {1} socks in {2} on {3}?"\
            .format(present_socks, sock_name, self.build_dirs["ctl"], host))

      #Create monolithic reference/replace dict
      repl_dict = dict(self.src_dirs, **self.build_dirs)
      repl_dict = dict(repl_dict, **sl2object)

      #Create gdbcmd from template
      gdbcmd_build_path = path.join(self.build_dirs["base"],
          "{0}.{1}.gdbcmd".format(res_bin_type, sl2object["id"]))

      new_gdbcmd = repl_file(repl_dict, gdbcmd_path)

      if new_gdbcmd:
        with open(gdbcmd_build_path, "w") as f:
          f.write(new_gdbcmd)
          f.close()
          log.debug("Wrote gdb cmd to {0}".format(gdbcmd_build_path))
          log.debug("Remote copying gdbcmd.")
          ssh.copy_file(gdbcmd_build_path, gdbcmd_build_path)
      else:
        log.fatal("Unable to parse gdb cmd at {1}!".format(gdbcmd_path))
        sys.exit(1)

      cmd = "sudo gdb -f -x {0} {1}".format(gdbcmd_build_path, self.src_dirs[res_bin_type])
      screen_sock_name = "sl.{0}.{1}".format(res_bin_type, sl2object["id"])

      #Launch slash2 in gdb within a screen session
      ssh.run_screen(cmd, screen_sock_name)

      #Wait two seconds to make sure slash2 launched without errors
      time.sleep(2)

      screen_socks = ssh.list_screen_socks()
      if screen_sock_name + "-error" in screen_socks or screen_sock_name not in screen_socks:
        log.fatal("sl2object {0}:{1} launched with an error. Resume to {2} and resolve it."\
            .format(sl2object["name"], sl2object["id"], screen_sock_name+"-error"))
        sys.exit(1)

      log.debug("Waiting for {0} sock on {1} to appear.".format(sock_name, host))
      while True:
        result = ssh.run(ls_cmd)
        if len(result["out"]) > 1:
          break
        time.sleep(1)
      ssh.close()

  def __sl_screen_and_wait(self, ssh, cmd, screen_name):
    """Common slash2 screen functionality.
    Check for existing sock, run the cmd, and wait to see if it timed out or executed successfully.

    Args:
      ssh: remote server connection.
      cmd: command to run remotely
      screen_name: name of screen sock to wait for."""

    #Run command string in screen
    if not ssh.run_screen(cmd, screen_name, self.conf["slash2"]["timeout"]):
      log.fatal("Screen session {0} already exists in some form! Attach and deal with it.".format(screen_name))
      sys.exit(1)

    wait = ssh.wait_for_screen(screen_name)

    if wait["timedout"]:
      log.critical("{0} timed out! screen -r {0}-timed and check it out."\
          .format(screen_name))
      sys.exit(1)
    elif wait["errored"]:
      log.critical("{0} exited with a nonzero return code. screen -r {0}-error and check it out."\
          .format(screen_name))
      sys.exit(1)

  def __stop_slash2_socks(self, sock_name, sl2objects, res_bin_type):
    """ Terminates all slash2 socks and screen socks on a generic host.
    Args:
      sock_name: name of sl2 sock.
      sl2objects: list of objects to be launched.
      res_bin_type: key to bin path in src_dirs.
      gdbcmd_path: path to gdbcmd file."""

    #res_bin_type NEEDS to be a path in src_dirs
    assert(res_bin_type in self.src_dirs)

    for sl2object in sl2objects:
      log.debug("Initializing environment > {0} @ {1}".format(sl2object["name"], sl2object["host"]))

      #Remote connection
      user, host = self.user, sl2object["host"]
      log.debug("Connecting to {0}@{1}".format(user, host))
      ssh = SSH(user, host, '')
      #ssh.kill_screens()

      cmd = "{0} -S {1}/{2}.{3}.sock stop".format(res_bin_type, self.build_dirs["ctl"], sock_name, host)
      log.debug(cmd)
      ssh.run(cmd)
      ssh.close()


  def parse_slash2_conf(self):
    """Reads and parses slash2 conf for tokens.
    Writes to the base directory; updates slash2 objects in the tsuite."""

    try:
      with open(self.conf["slash2"]["conf"]) as conf:
        new_conf = "#TSuite Slash2 Conf\n"

        res, site_name = None, None
        in_site = False
        site_id, fsuuid = -1, -1
        client = None

        #Regex config parsing for sl2objects
        reg = {
          "client" : re.compile(
            "^#client\s*=\s*(.+?)\s*;\s*$"
          ),
          "type"   : re.compile(
            "^\s*?type\s*?=\s*?(\S+?)\s*?;\s*$"
          ),
          "id"     : re.compile(
            "^\s*id\s*=\s*(\d+)\s*;\s*$"
          ),
          "zpool"  : re.compile(
            r"^\s*?#\s*?zfspool\s*?=\s*?(\w+?)\s+?(.*?)\s*$"
          ),
          "zpool_path"  : re.compile(
            r"^\s*?#\s*?zfspath\s*?=\s*?(.+?)\s*$"
          ),
          "prefmds": re.compile(
            r"\s*?#\s*?prefmds\s*?=\s*?(\w+?@\w+?)\s*$"
          ),
          "fsuuid": re.compile(
            r"^\s*set\s*fsuuid\s*=\s*\"?(0x[a-fA-F\d]+|\d+)\"?\s*;\s*$"
          ),
          "fsroot" : re.compile(
            "^\s*?fsroot\s*?=\s*?(\S+?)\s*?;\s*$"
          ),
          "nids"    : re.compile(
            "^\s*?nids\s*?=\s*?(.*)$"
          ),
          "new_res": re.compile(
            "^\s*resource\s+(\w+)\s*{\s*$"
          ),
          "fin_res": re.compile(
            "^\s*?}\s*$"
          ),
          "site"   : re.compile(
            "^\s*?site\s*?@(\w+).*?"
          ),
          "site_id": re.compile(
            "^\s*site_id\s*=\s*(0x[a-fA-F\d]+|\d+)\s*;\s*$"
          )
        }

        line = conf.readline()

        while line:
          #Replace keywords and append to new conf

          new_conf += repl(self.build_dirs, line)

          #Iterate through the regexes and return a tuple of
          #(name, [\1, \2, \3, ...]) for successful matches

          matches = [
            (k, reg[k].match(line).groups()) for k in reg\
            if reg[k].match(line)
          ]

          #Should not be possible to have more than one
          assert(len(matches) <= 1)

          #log.debug("%s %s %s\n->%s" % (matches, in_site, res, line))
          if matches:
            (name, groups) = matches[0]

            if in_site:

              if name == "site_id":
                site_id = groups[0]

              elif res:
                if name == "type":
                  res["type"] = groups[0]

                elif name == "id":
                  res["id"] = groups[0]

                elif name == "zpool_path":
                  res["zpool_path"] = groups[0].strip()

                elif name == "zpool":
                  res["zpool_name"] = groups[0]
                  res["zpool_cache"] = path.join(
                    self.build_dirs["base"], "{0}.zcf".format(groups[0])
                  )
                  res["zpool_args"] = groups[1]

                elif name == "prefmds":
                  res["prefmds"] = groups[0]


                elif name == "fsroot":
                  res["fsroot"] = groups[0].strip('"')

                elif name == "nids":
                  #Read subsequent lines and get the first host

                  tmp = groups[0]
                  while line and ";" not in line:
                    tmp += line
                    line = conf.readline()
                  tmp = re.sub(";\s*$", "", tmp)
                  res["host"] = re.split("\s*,\s*", tmp, 1)[0].strip(" ")

                elif name == "fin_res":
                  #Check for errors finalizing object
                  res["site_id"] = site_id
                  res["fsuuid"] = fsuuid

                  if not res.finalize(self.sl2objects):
                    sys.exit(1)
                  res = None
              else:
                if name == "new_res":
                  res =  SL2Res(groups[0], site_name)
            else:
              if name == "client":
                self.clients = [g.strip() for g in groups[0].split(",")]
              if name == "site":
                site_name = groups[0]
                in_site = True
              elif name == "fsuuid":
                fsuuid = groups[0]

          line = conf.readline()

        new_conf_path = path.join(self.build_dirs["base"], "slash.conf")

        try:
          with open(new_conf_path, "w") as new_conf_file:
            new_conf_file.write(new_conf)
            log.debug("Successfully wrote build slash2 conf at {0}"\
                .format(new_conf_path))
            for sl2_obj in self.all_objects():
              ssh = SSH(self.user, sl2_obj["host"], '')
              log.debug("Copying new config to {0}".format(sl2_obj["host"]))
              ssh.copy_file(new_conf_path, new_conf_path)
              ssh.close()
        except IOError, e:
          log.fatal("Unable to write new conf to build directory!")
          log.fatal(new_conf_path)
          log.fatal(e)
          return False
    except IOError, e:
      log.fatal("Unable to read conf file at {0}"\
          .format(self.conf["slash2"]["conf"]))
      log.fatal(e)
      return False

    return True

  def mk_dirs(self, dirs):
    """Creates directories and subdirectories.
    Does not consider file exists as an error.

    Args:
      dirs: list of directory paths.
    Returns:
      False if there was an error.
      True if everything executed."""

    for d in dirs:
      try:
        os.makedirs(d)
      except OSError, e:

        #Error 17 is that the file exists
        #Should be okay as the dir dictionary
        #does not have a guarnteed ordering

        if e.errno != 17:
          log.fatal("Unable to create: {0}".format(d))
          log.fatal(e)
          return False
    return True

  def replace_rel_dirs(self, dirs, lookup = None):
    """Looks up embedded keywords in a dict.

    Args:
      dirs: dict with strings to parse.
      lookup: dict in which keywords are located. If None, looks up in dirs."""

    if lookup is None:
      lookup = dirs

    for k in dirs:
      #Loop and take care of embedded lookups
      replaced = repl(lookup, dirs[k])
      while replaced != dirs[k]:
        dirs[k] = replaced
        replaced = repl(lookup, dirs[k])

  def build_base_dir(self):
    """Generates a valid, non-existing directory for the TSuite."""

    #Assemble a random test directory base
    tsid = randrange(1, 1 << 24)
    random_build = "sltest.{0}".format(tsid)
    base = path.join(self.rootdir, random_build)

    #Call until the directory doesn't exist
    if path.exists(base):
      self.build_base_dir()
    else:
      self.tsid = tsid
      self.build_dirs["base"] = base

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

def check_subset(necessary, check):
  """Determines missing elements from necessary list.

  Args:
    necessary: list of mandatory objects.
    check: list to be checked.
  Returns:
    List of elements missing from necessary."""

  if not all(n in check for n in necessary):
    #Remove sections that are correctly there
    present = [s for s in check if s in necessary]
    map(necessary.remove, present)
    return necessary
  return []

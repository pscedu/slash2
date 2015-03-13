import logging, re, os, sys
import glob, time, base64
import json, signal

from random import randrange
from os import path
from os import chmod
from paramiko import SSHException

from utils.sl2 import SL2Res
from utils.ssh import SSH
from managers.sl2gen import repl, repl_file

from managers import sl2gen, mds, ion, mnt

log = logging.getLogger("sl2.ts")

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

      #TODO: bring this into the configuration file
      # Or, on a per reosurce basis
      "mp"   : "%base%/mp",

      "datadir": "%base%/data",
      "ctl"  : "%base%/ctl",
      "fs"   : "%base%/fs"
    }

    #Determine where the module is being ran
    self.cwd = path.realpath(__file__).split(os.sep)[:-1]
    self.cwd = os.sep.join(self.cwd)
    self.authbuf_key = None

    self.src_dirs = {
      # "src" populated in init
      "slbase"  : "%src%/slash2",
      "tsbase"  : "%slbase%/../tsuite",
      "zpool"   : "%src%/zfs/src/cmd/zpool/zpool",
      "zfs_fuse": "%slbase%/utils/zfs-fuse.sh",
      "sliod"   : "%slbase%/sliod/sliod",
      "slmkjrnl": "%slbase%/slmkjrnl/slmkjrnl",
      "slmctl"  : "%slbase%/slmctl/slmctl",
      "slictl"  : "%slbase%/slictl/slictl",
      "slashd"  : "%slbase%/slashd/slashd",
      "slkeymgt": "%slbase%/slkeymgt/slkeymgt",
      "slmkfs"  : "%slbase%/slmkfs/slmkfs",
      "mount_slash" : "%slbase%/mount_slash/mount_slash",
      "msctl" : "%slbase%/msctl/msctl"
    }

    self.tsid = None
    self.rootdir = None

    self.sl2objects = {}
    self.conf = conf

    self.user = os.getenv("USER")

    #TODO: Rename rootdir in src_dir fashion
    self.rootdir = self.conf["tsuite"]["rootdir"]
    self.src_dirs["src"] = self.conf["source"]["srcroot"]

    self.local_setup()
    self.create_remote_setups()

    #register a cleanup exit method
    def exit_handler(signal, frame):
      log.critical("User killing tsuite!")
      self.shutdown()

    signal.signal(signal.SIGINT, exit_handler)


  def all_objects(self):
    """Returns all sl2objects in a list."""

    objects = []
    for res, res_list in self.sl2objects.items():
      objects.extend(res_list)
    return objects

  #TODO: proc fs doesn't exist in BSD :(
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

  def local_setup(self):
    """Create the local build directories and parse the slash2 config."""

    #Necessary to compute relative paths
    self.build_base_dir()
    log.debug("Base directory: {0}".format(self.build_dirs["base"]))

    self.replace_rel_dirs(self.build_dirs)

    if not self.mk_dirs(self.build_dirs.values()):
      log.fatal("Unable to create some necessary directory!")
      self.shutdown()
    log.info("Successfully created build directories")
    os.system("chmod -R 777 \"{0}\"".format(self.build_dirs["base"]))

    #Compute relative paths for source dirs
    self.replace_rel_dirs(self.src_dirs)

    #Also check for embedded build paths
    self.replace_rel_dirs(self.src_dirs, self.build_dirs)

    if not self.parse_slash2_conf():
      log.critical("Error parsing slash2 configuration file!")
      self.shutdown()

    log.info("slash2 configuration parsed successfully.")

    #Show the resources parsed
    objs_disp = [
      "{0}:{1}".format(res, len(res_list))\
          for res, res_list in self.sl2objects.items()
    ]
    log.debug("Found: {0}".format(", ".join(objs_disp)))

  def create_remote_setups(self):
    """Create the necessary build directories on all slash2 objects."""

    for sl2_obj in self.all_objects():
      try:
        ssh = SSH(self.user, sl2_obj["host"], '')
        log.debug("Creating build directories on {0}@{1}".format(sl2_obj["name"], sl2_obj["host"]))
        for d in self.build_dirs.values():
          ssh.make_dirs(d)
          ssh.run("sudo chmod -R 777 \"{0}\"".format(d), quiet=True)
        ssh.close()
      except SSHException:
        log.error("Unable to connect to {0} to create build directories!".format(sl2_obj["host"]))

  def store_report(self):
    """Populates test report with initial/available information.
    Must be ran after run_tests"""

    try:
      from utils.mongohelper import MongoHelper
      self.mongo = MongoHelper(self.conf)
      pass
    except Exception:
      log.critical("Unable to connect to mongodb.")
      self.shutdown()

    #Resource information

    test_report = {}

    test_report["resources"] = self.sl2objects

    test_report["total_time"] = 0.0
    test_report["total_tests"] = 0
    test_report["failed_tests"] = 0
    test_report["tests"] = []

    latest_tset = self.mongo.get_latest_tset()
    test_report["tsid"] = latest_tset["tsid"]+1 if latest_tset else 1
    test_report["tset_name"] = "#" + str(test_report["tsid"])

    for test, clients in self.test_results.items():
      for client in clients:
        test_report["total_tests"] += 1
        if not client["result"]["pass"]:
          test_report["failed_tests"] += 1
        test_report["total_time"] += client["result"]["elapsed"]

    test_report["tests"] = self.test_results
    self.mongo.col.save(test_report)


  def run_tests(self):
    """Uploads and runs each test on each client."""

    tset_dir = self.conf["tests"]["runtime_tsetdir"]

    if len(self.sl2objects["client"]) == 0:
      log.error("No test clients?")
      return

    client_hosts = [client["host"] for client in self.sl2objects["client"]]

    ssh_clients = []
    for host in client_hosts:
      try:
        ssh_clients.append(SSH(self.user, host))
      except SSHException:
        log.error("Unable to connect to %s@%s", self.user, host)

    remote_tests_path = path.join(self.build_dirs["mp"], "tests")
    map(lambda ssh: ssh.make_dirs(remote_tests_path, escalate=True), ssh_clients)

    tests = []
    for test in os.listdir(tset_dir):
      test_path = path.join(tset_dir, test)
      if os.access(test_path, os.X_OK): #is executable
        tests.append(test)
        remote_test_path = path.join(remote_tests_path, test)
        map(lambda ssh: ssh.copy_file(test_path, remote_test_path), ssh_clients)
        map(lambda ssh: ssh.run("sudo chmod +x {0}".format(remote_test_path)), ssh_clients)
    log.debug("Found tests: {0}".format(", ".join(tests)))

    test_handler_path = path.join(self.cwd, "handlers", "test_handle.py")
    remote_test_handler_path = path.join(self.build_dirs["mp"], "test_handle.py")

    ssh_path = path.join(self.cwd, "utils" , "ssh.py")
    remote_ssh_path = path.join(self.build_dirs["mp"], "ssh.py")

    map(lambda ssh: ssh.copy_file(test_handler_path, remote_test_handler_path, elevated=True), ssh_clients)
    map(lambda ssh: ssh.copy_file(ssh_path, remote_ssh_path, elevated=True), ssh_clients)

    sock_name = "sl2.{0}.tset".format(self.conf["tests"]["runtime_tsetname"])

    killed_clients = sum(map(lambda ssh: ssh.kill_screens(sock_name, exact_sock=True), ssh_clients))
    if killed_clients > 0:
      log.debug("Killed {0} stagnant tset sessions. Please take care of them next time.".format(killed_clients))

    log.debug("Running tests on clients.")

    # create list of daemons running and their information to be sent to test_handler
    daemons = []
    for object_type in self.sl2objects.keys():
      for sl2object in self.sl2objects[object_type]:
        if "pid" in sl2object:
          if object_type == "mds":      ctl_path = self.src_dirs['slmctl']
          elif object_type == "ion":    ctl_path = self.src_dirs['slictl']
          elif object_type == "client": ctl_path = self.src_dirs['msctl']

          daemons.append((sl2object["host"], object_type, ctl_path, sl2object["pid"]))

    runtime = {"build_dirs": self.build_dirs, "daemons" : daemons}
    runtime_arg = base64.b64encode(json.dumps(runtime))

    map(lambda ssh: ssh.run_screen("python2 {0} {1} > ~/log".format(remote_test_handler_path, runtime_arg),
        sock_name, quiet=True), ssh_clients)

    if not all(map(lambda ssh: ssh.wait_for_screen(sock_name)["finished"], ssh_clients)):
      log.error("Some of the screen sessions running the tset encountered errors! Please check out the clients and rectify the issue.")
      self.shutdown()

    result_path = path.join(self.build_dirs["base"], "results.json")

    self.test_results = {}
    try:
      for ssh in ssh_clients:
        results = json.loads(ssh.run("cat "+result_path, quiet=True)["out"][0])
        for test in results:
          if test["name"] not in self.test_results:
            self.test_results[test["name"]] = []
          self.test_results[test["name"]].append({"client": ssh.host, "result": test})
      print json.dumps(self.test_results, indent=True)
    except Exception as e:
      print e
      log.critical("Tests did not return output!")
      self.shutdown()

    map(lambda ssh: ssh.close(), ssh_clients)

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
          "clients" : re.compile(
            "^\s*?#\s*clients\s*=\s*(.+?)\s*;\s*$"
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
            "^\s*?fsroot\s*?=\s*?(\S+?)\s*?;\s*$"),
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
          ),
          "jrnldev": re.compile(
            "^\s*jrnldev\s*=\s*([/\w]+)\s*;\s*$"
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

                elif name == "jrnldev":
                  res["jrnldev"] = groups[0]

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
                    self.shutdown()
                  res = None
              else:
                if name == "new_res":
                  res =  SL2Res(groups[0], site_name)
            else:
              if name == "clients":
                for client in [g.strip() for g in groups[0].split(",")]:
                  client_res = SL2Res(client, None)
                  client_res["type"] = "client"
                  client_res["host"] = client
                  client_res.finalize(self.sl2objects)
              elif name == "site":
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
              try:
                ssh = SSH(self.user, sl2_obj["host"], "")
                log.debug("Copying new config to {0}".format(sl2_obj["host"]))
                try:
                  ssh.copy_file(new_conf_path, new_conf_path)
                except IOError:
                  log.critical("Error copying config file to {0}".format(ssh.host))
                  self.shutdown()
                ssh.close()
              except SSHException:
                log.error("Unable to copy config file to {0}!".format(sl2_obj["host"]))
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

  def shutdown(self, ignore = None):
    """Shuts down test suite process, stops all sl2 objects and exits
    Args:
      ingore_list: dict with strings to parse.
    """

    log.info("Shutting down Tsuite objects")
    if ignore != None:
      log.info("Ignoring shutdown of {0} for debug purposes.".format(ignore))

    if "client" in self.sl2objects and "client" != ignore:
      mnt.kill_mnt(self)
    if "mds" in self.sl2objects and "mds" != ignore:
      mds.kill_mds(self)
    if "ion" in self.sl2objects and "ion" != ignore:
      ion.kill_ion(self)

    log.info("Exiting tsuite.")
    sys.exit(1)

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

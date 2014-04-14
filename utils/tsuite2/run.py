import logging, sys, os, re

#from colorama import init, Fore
from argparse import ArgumentParser
from ConfigParser import ConfigParser

from tsuite import TSuite, check_subset

from managers.mds import *
from managers.ion import *
from managers.mnt import *

log = logging.getLogger("sl2")

def main():
  """Entry point into the SLASH2 Test Suite.
  Deals with argument parsing and main configuration."""

  #Reset colorama after prints
  #  init(autoreset=True)

  parser = ArgumentParser(description="SLASH2 Test Suite")
  parser.add_argument("-v", "--verbose", action="count",
    help="increase verbosity", default=0)
  parser.add_argument("-l", "--log-file", help="log output to a file",
    default=None)
  parser.add_argument("-c", "--config-file",
    help="path to slash2 test suite config",
    default="tsuite.conf")
  parser.add_argument("-s", "--source", choices=["src", "svn"],
    help="build from src or svn", default="src")
  parser.add_argument("-x", "--ignore-tests", help="ignore a test set, folder name", default=[], nargs="*")
  parser.add_argument("-i", "--ignore", help="list of processes to ignore. Defaults to none.", default=[], nargs="*")
  parser.add_argument("-a", "--only",   help="list of processes to perform. Defaults to all.", default=[], nargs="*")
  parser.add_argument("-o", "--overrides", help="Override value in config. -s section:value=something ...",
    nargs="+")

  args = parser.parse_args()

  #Get log level
  level = [logging.WARNING, logging.INFO, logging.DEBUG]\
      [2 if args.verbose > 2 else args.verbose]

  log.setLevel(level)

  logging.basicConfig(level=level,
    format='%(asctime)s %(name)-8s %(levelname)-8s %(message)s',
    datefmt='%H:%M'
  )

  #Setup file log
  if args.log_file:
    fch = logging.FileHandler(args.log_file)
    fch.setLevel(level)
    fch.setFormatter(
      logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    log.addHandler(fch)

  #Check for config file
  conf = ConfigParser()

  if len(conf.read(args.config_file)) == 0:
    log.fatal("Unable to read configuration file!")
    sys.exit(1)

  #Required sections; check for their existence

  sections = {
    "tsuite": [
      "rootdir",
      "logbase"
    ],
    "slash2": [
      "conf",
      "mds_gdb", "ion_gdb", "mnt_gdb"
    ],
    "tests": [
      "testdir",
      "excluded"
    ]
  }

  #Building from source or svn

  if args.source == "svn":
    sections["svn"] = [
      "svnroot"
    ]
  else:
    sections["source"] = [
      "srcroot"
    ]

  #Apply configuration overrides
  if args.overrides:
    overreg = re.compile(r"^(\w+):(\w+)=(.+?)$")
    for override in args.overrides:
      match = overreg.match(override)
      if match:
        section, key, value = match.groups()
        if section not in conf._sections or key not in conf._sections[section]:
          print "Override {0} does not override an existing config value!".format(override)
          sys.exit(1)
        conf._sections[section][key] = value

  #Check that the required sections exist
  missing = check_subset(list(sections), list(conf._sections))
  if len(missing) != 0:
    log.fatal("Configuration file is missing sections!")
    log.fatal("Missing: {}".format(", ".join(missing)))
    sys.exit(1)

  #Check that all the fields listed are present
  #in each section
  for section in sections:
    missing = check_subset(
      sections[section],
      conf._sections[section]
    )
    if len(missing) != 0:
      log.fatal("Missing fields in {} section!".format(section))
      log.fatal("Missing: {}".format(", ".join(missing)))
      sys.exit(1)

  if "timeout" not in conf._sections["slash2"]:
    conf._sections["slash2"]["timeout"] = None

  log.info("Configuration file loaded successfully!")

  testdir = conf._sections["tests"]["testdir"]
  tests = []
  try:
    #Consider all directories in test_dir to have tests to be ran

    tests = [test for test in os.listdir(testdir) if os.path.isdir(os.path.join(testdir, test))]
  except OSError, e:
    log.critical("Unable to gather test sets from the testing directory!")
    sys.exit(1)

  if len(tests) == 0:
    log.critical("No test sets found.")

  for test in tests:
    runtime_testdir = os.path.join(conf._sections["tests"]["testdir"], test)
    ignore_file = os.path.join(runtime_testdir, ".ignore")
    slash_conf = os.path.join(runtime_testdir, "slash.conf")
    if os.path.isfile(ignore_file) or test in args.ignore_tests:
      log.debug("Ignoring {0}".format(runtime_testdir))
      continue
    elif os.path.isfile(slash_conf):
      log.debug("Replaced default slash config with {0} for this test set".format(slash_conf))
      conf._sections["slash2"]["conf"] = slash_conf

    conf._sections["tests"]["runtime_testdir"] = runtime_testdir
    conf._sections["tests"]["runtime_testname"] = test

    condition = True

    #Only will enumerate the only items to do
    if args.only:
      condition = False

    processes = {
      (1, "create"): {
        (1, "mds"): condition,
        (2, "ion"): condition
      },
      (2, "launch"): {
        (1, "mds"): condition,
        (2, "ion"): condition,
        (3, "mnt"): condition
      },
      (3, "run"): {
        (1, "tests"): condition
      },
      (4, "kill"): {
        (3, "mds"): False,
        (2, "ion"): False,
        (1, "mnt"): False,
        (4, "all"): condition
      }
    }

    change_items(True, processes, args.only)
    change_items(False, processes, args.ignore)

    #Initialize the test suite
    t = TSuite(conf._sections)

    for parent in sorted(processes.keys(), key=lambda x: x[0]):
      for item in sorted(processes[parent].keys(), key=lambda x: x[0]):
        state = processes[parent][item]
        if state:
          parent_lookup = parent[1]
          item_lookup = item[1]

          if item_lookup == "mds":
            if parent_lookup == "create":
              create_mds(t)
            elif parent_lookup == "launch":
              launch_mds(t)
            elif parent_lookup == "kill":
              kill_mds(t)
          elif item_lookup == "ion":
            if parent_lookup == "create":
              create_ion(t)
            elif parent_lookup == "launch":
              launch_ion(t)
            elif parent_lookup == "kill":
              kill_ion(t)
          elif item_lookup == "mnt":
            if parent_lookup == "launch":
              launch_mnt(t)
            elif parent_lookup == "kill":
              kill_mnt(t)
          elif item_lookup == "tests" and parent_lookup == "run":
            t.run_tests()
          elif item_lookup == "all" and parent_lookup == "kill":
            t.shutdown()


def get_parent_tuple(value, items):
  """Find the parent tuple of a value in a list.

  Args:
    value: value to lookup.
    items: dict to look up in.

  Returns: tuple key."""

  for (priority, key) in items:
    if key == value:
      return (priority, key)
  return

def change_items(condition, processes, items):
  """Changes the state of the processes.

  Args:
    condition: true or false to set on processes
    processes: dict containing processes.
    items: list of key:value pairs."""

  itemreg = re.compile(r"^(\w+):(\w+)$")

  for item in items:
    match = itemreg.match(item)
    if match:
      parent, key = match.groups()
      parent_tuple = get_parent_tuple(parent, processes.keys())
      if parent_tuple not in processes:
        log.critical("%s does not refer to a valid parent action.", parent)
        sys.exit(1)
      key_tuple = get_parent_tuple(key, processes[parent_tuple].keys())
      if key_tuple not in processes[parent_tuple]:
        log.critical("%s does not refer to a valid process to perform.", key)
        sys.exit(1)
      processes[parent_tuple][key_tuple] = condition
    else:
      log.critical("%s is not of the valid parent:key form.", item)
      sys.exit(1)

if __name__=="__main__":
  main()


import logging, sys, os

#from colorama import init, Fore
from argparse import ArgumentParser
from ConfigParser import ConfigParser

from tsuite import TSuite, check_subset

log = logging.getLogger("slash2")

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
  parser.add_argument("-b", "--build", choices=["src", "svn"],
    help="build from src or svn", default="src")

  args = parser.parse_args()

  #Get log level
  level = [logging.WARNING, logging.INFO, logging.DEBUG]\
      [2 if args.verbose > 2 else args.verbose]

  log.setLevel(level)

  #Setup stream log (console)
#  fmt_string = "{2}%(asctime)s{0} [{1}%(levelname)s{0}] {2}%(message)s"\
#    .format(Fore.RESET, Fore.CYAN, Fore.WHITE)

  fmt_string = "%(asctime)s} [%(levelname)s] %(message)s"
  ch = logging.StreamHandler()
  ch.setLevel(level)
  ch.setFormatter(logging.Formatter(fmt_string))

  log.addHandler(ch)

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

  if args.build == "svn":
    sections["svn"] = [
      "svnroot"
    ]
  else:
    sections["source"] = [
      "srcroot"
    ]

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

  for test in tests:
    runtime_testdir = os.path.join(conf._sections["tests"]["testdir"], test)
    slash_conf = os.path.join(runtime_testdir, "slash.conf")
    if os.path.isfile(slash_conf):
      log.debug("Replaced default slash config with {1} for this test set".format(slash_conf))
      conf._sections["slash2"]["conf"] = slash_conf

    conf._sections["tests"]["runtime_testdir"] = runtime_testdir

    #Initialize the test suite
    t = TSuite(conf._sections)
    t.build_mds()
    t.launch_mds()
    t.build_ion()
    t.launch_ion()


if __name__=="__main__":
  main()


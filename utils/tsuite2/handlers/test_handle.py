import sys, json, pkgutil, time
import shutil, base64, json
import os
from ssh import SSH
from os import path

class TestHandler(object):
  """Object used to set up testing environment and passed along to tests for runtime information."""

  def __init__(self, json_constants):
    self.runtime = json.loads(base64.b64decode(json_constants))

    #Determine where the module is being ran
    self.cwd = path.realpath(__file__).split(os.sep)[:-1]
    self.cwd = os.sep.join(self.cwd)

    self.modules_folder = path.join(self.cwd, "modules")
    self.modules = self.load_all_modules_from_dir(self.modules_folder)

    self.run_tests()
    self.cleanup()

  def get_resource_usages(self):
      #query each slash2 compenent for resource usage
      for host, daemon_path, pid in self.runtime["daemons"]:
          user = os.getenv("USER")
          ssh = SSH(user, host, '');
          ssh.run("sudo kill -s SIGUSR1 {0}".format(pid)) # linux
          log = open("/home/beckert/log", "w")
          log.write(str(ssh.run("sudo {0} -S {1}/*.sock -s rusage".format(daemon_path, self.runtime["build_dirs"]["ctl"])))) #bsd
          log.close()

  def run_tests(self):
    """Run all tests from the tests directory and print results"""
    tset_results = {"tests": []}
    for module in self.modules:
      test = {"name": module.__name__.split(".")[-1]}
      print "Running", test["name"]
      test["setup"]=module.setup()

      start = time.time()
      test["operate"]=module.operate()
      elapsed = time.time() - start
      test["operate"]["elapsed"] = elapsed

      test["cleanup"]=module.cleanup()

      test["resource_usage"]=self.get_resource_usages()

      tset_results["tests"].append(test)

    results_file = path.join(self.cwd, "results.json")
    f = open(results_file, "w")
    f.write(json.dumps(tset_results))
    f.close()

  def cleanup(self):
    shutil.rmtree(self.modules_folder)

  def load_all_modules_from_dir(self, dirname):
    modules = []
    for importer, package_name, _ in pkgutil.iter_modules([dirname]):
      full_package_name = '%s.%s' % (dirname, package_name)
      if full_package_name not in sys.modules:
        module = importer.find_module(package_name).load_module(full_package_name)
        modules.append(module)
    return modules

if __name__ == "__main__":
  #Ran from script
  TestHandler(sys.argv[1])


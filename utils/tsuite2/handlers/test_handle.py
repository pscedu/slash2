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

  def query_ctl_rusage(self, ssh, ctl_path):
     output = ssh.run("sudo {0} -S {1}/*.sock -s rusage".format(ctl_path, self.runtime["build_dirs"]["ctl"])) #use the ctl daemons' resource usage
     return {"output":output}

  def check_status(self, ssh, ctl_path):
    """Generate general status report for all sl2 objects.

    Returns: {
      "load": ..., "disk_stats": ... "
      ...
    }"""

    #Operations based on type
    ops = {
        "all": {
          "load": "cat /proc/loadavg | cut -d' ' -f1,2,3",
          "mem_total": "cat /proc/meminfo | head -n1",
          "mem_free": "sed -n 2,2p /proc/meminfo",
          "uptime": "cat /proc/uptime | head -d' ' -f1",
          "disk_stats": "df -hl"
        },
        "slmctl": {
          "connections":"sudo {0} -S {1}/slashd.*.sock -sconnections",
          "iostats":"sudo {0} -S {1}/slashd.*.sock -siostats"
        },
        "slictl": {
          "connections": "sudo {0} -S {1}/sliod.*.sock -sconnections",
          "iostats": "sudo {0} -S {1}/sliod.*.sock -siostats"
        }
    }

    report = {}

    obj_ops = ops["all"]
    if path.basename(ctl_path) in ops:
      obj_ops = dict(ops["all"].items() + ops[path.basename(ctl_path)].items())

    for op, cmd in obj_ops.items():
      if "{0}" in cmd and "{1}" in cmd: #needs the paths
        cmd = cmd.format(ctl_path, self.runtime["build_dirs"]["ctl"])
      report[op] = ssh.run(cmd, timeout=2)

    print "Status check completed for {0}".format(ssh.host)

    return report

  def get_resource_usages(self):
    #query each slash2 compenent for resource usage
    rusages = {}
    for host, ctl_path, pid in self.runtime["daemons"]:
      user = os.getenv("USER")
      ssh = SSH(user, host, '');
      kernel = "".join(ssh.run("uname -s")["out"]).lower()
      if "linux" in kernel:
        output = self.check_status(ssh, ctl_path)
      elif "bsd" in kernel:
        output = self.query_ctl_rusage(ssh, ctl_path)
      else:
        pass

      log = open("/home/beckert/log", "w")
      log.write(str(output) + "\n")
      log.close()

      rusages["{0} [{1}]".format(host, pid)] = output

    return rusages

  def run_tests(self):
    """Run all tests from the tests directory and print results"""
    tset_results = []
    for module in self.modules:
      test = {"name": module.__name__.split(".")[-1]}
      print "Running", test["name"]

      #TODO: do something with this
      #test["setup"]=module.setup()
      #test["cleanup"]=module.cleanup()

      start = time.time()
      test["operate"]=module.operate()
      test["operate"]["elapsed"] = time.time() - start

      test["resource_usage"]=self.get_resource_usages()
      tset_results.append(test)

    # user may not have write privileges to mp -- hacky mv
    results_file = path.join(self.cwd, "results.json")
    temp_file = path.join("/tmp", "results.json")
    f = open(temp_file, "w")
    f.write(json.dumps(tset_results))
    f.close()
    os.system("sudo mv {0} {1}".format(temp_file, results_file))

  def cleanup(self):
    pass
    # TODO: we don't have permissions to delete these..
    #shutil.rmtree(self.modules_folder)

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

import sys, json, pkgutil, time
import shutil, base64, json
import os, glob, subprocess
from ssh import SSH
from os import path

class TestHandler(object):
  """Object used to set up testing environment and passed along to tests for runtime information."""

  def __init__(self, json_constants):
    self.runtime = json.loads(base64.b64decode(json_constants))

    #Determine where the test is being ran
    self.cwd = path.realpath(__file__).split(os.sep)[:-1]
    self.cwd = os.sep.join(self.cwd)

    self.tests_dir = path.join(self.cwd, "tests")

    # Write runtime information to a file for tests to access
    self.runtime_file = path.join(self.runtime["build_dirs"]["base"], "runtime.json")
    rt_file = open(self.runtime_file, 'w')
    rt_file.write(json.dumps(self.runtime))
    rt_file.close()

    self.run_tests()
    self.cleanup()


  def query_ctl_rusage(self, ssh, type, ctl_path):
    """ NOT IMPLEMENTED """
    output = ssh.run("sudo {0} -S {1}/*.sock -s rusage".format(ctl_path, self.runtime["build_dirs"]["ctl"])) #use the ctl daemons' resource usage
    return {"output":output}

  def check_status(self, ssh, type, ctl_path, pid):
    """Generate general status report for a sl2 object.  """

    #Operations based on type
    base_ops = {
        "machine": {
          "load": "cat /proc/loadavg | cut -d' ' -f1,2,3",
          "meminfo": "cat /proc/meminfo | head -n1 | cut -c 18-",
          "cpu_uptime": "cat /proc/uptime | cut -d' ' -f1"
        },
        "daemon":{
          "type":"echo {0}".format(type),
          "mem_usage": "cat /proc/{0}/status | grep VmSize | cut -f2",
          "mem_peak": "cat /proc/{0}/status | grep VmPeak | cut -f2",
          "threads": "cat /proc/{0}/status | grep Threads | cut -f2"
        },
    }

    mds_ops = {
      "connections":"sudo {0} -S {1}/slashd.*.sock -sconnections | wc -l"
    }

    ion_ops = {
      "connections": "sudo {0} -S {1}/sliod.*.sock -sconnections | wc -l"
    }

    client_ops = {
    }

    report = {}

    for set, checks in base_ops.items():
      report[set] = {}
      for op, cmd in checks.items():
        if "{0}" in cmd: #needs pid
          cmd = cmd.format(pid)
        report[set][op] = "".join(ssh.run(cmd, timeout=2)["out"]).strip()

    if type == "mds":
      for op, cmd in mds_ops.items():
        if "{0}" in cmd and "{1}" in cmd: #needs the paths
          cmd = cmd.format(ctl_path, self.runtime["build_dirs"]["ctl"])
        report[set][op] = "".join(ssh.run(cmd, timeout=2)["out"]).strip()
    elif type == "ion":
      for op, cmd in ion_ops.items():
        if "{0}" in cmd and "{1}" in cmd: #needs the paths
          cmd = cmd.format(ctl_path, self.runtime["build_dirs"]["ctl"])
        report[set][op] = "".join(ssh.run(cmd, timeout=2)["out"]).strip()

    print "Status check completed for {0}".format(ssh.host)
    return report

  def get_resource_usages(self):
    #query each slash2 compenent for resource usage
    rusages = {}
    for host, type, ctl_path, pid in self.runtime["daemons"]:
      print type
      user = os.getenv("USER")
      ssh = SSH(user, host, '');
      kernel = "".join(ssh.run("uname -s")["out"]).lower()
      if "linux" in kernel:
        output = self.check_status(ssh, type, ctl_path, pid)
      elif "bsd" in kernel:
        output = self.query_ctl_rusage(ssh, type, ctl_path)
      else:
        # do something
        pass
      rusages[host] = output

    return rusages
    sys.exit(1)

  def run_tests(self):
    """Run all tests from the tests directory and print results"""
    tset_results = []
    available_tests = [file for file in glob.glob(self.tests_dir + "/*") if os.access(file, os.X_OK)]
    for test_file in available_tests:
      results = {"name": test_file.split(".")[-2].split("/")[-1]}
      print "Running", results["name"]

      start = time.time()
      results["retcode"] = subprocess.call([test_file, self.runtime_file])
      results["pass"] = results["retcode"] == 0
      results["elapsed"] = time.time() - start

      results["rusage"] = self.get_resource_usages()
      tset_results.append(results)

    results_file = path.join(self.runtime["build_dirs"]["base"], "results.json")
    f = open(results_file, "w")
    f.write(json.dumps(tset_results))
    f.close()

  def cleanup(self):
    # TODO: we don't have permissions to delete these..
    #shutil.rmtree(self.modules_folder)
    pass

if __name__ == "__main__":
  #Ran from script
  TestHandler(sys.argv[1])

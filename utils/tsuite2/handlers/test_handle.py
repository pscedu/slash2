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


  def query_ctl_rusage(self, ssh, ctl_path):
    """ NOT IMPLEMENTED """
    output = ssh.run("sudo {0} -S {1}/*.sock -s rusage".format(ctl_path, self.runtime["build_dirs"]["ctl"])) #use the ctl daemons' resource usage
    return {"output":output}

  def check_status(self, ssh, ctl_path, pid):
    """Generate general status report for a sl2 object.  """

    #Operations based on type
    base_ops = {
        "machine": {
          "load": "cat /proc/loadavg | cut -d' ' -f1,2,3",
          "meminfo": "cat /proc/meminfo | head -n1",
          "cpu_uptime": "cat /proc/uptime | cut -d' ' -f1",
          "disk_stats": "df -hl | grep sltest"
        },
        "daemon":{
          "mem_usage": "cat /proc/{0}/status | grep VmSize | cut -f2",
          "mem_peak": "cat /proc/{0}/status | grep VmPeak | cut -f2",
          "threads": "cat /proc/{0}/status | grep Threads | cut -f2"
        },
    }

    mds_ops = {
      "connections":"sudo {0} -S {1}/slashd.*.sock -sconnections | wc -l",
      "iostats":""#sudo {0} -S {1}/slashd.*.sock -siostats | wc "
    }

    ion_ops = {
      "connections": "sudo {0} -S {1}/sliod.*.sock -sconnections | wc -l",
      "iostats": ""#sudo {0} -S {1}/sliod.*.sock -siostats"
    }

    report = {}

    for set, checks in base_ops.items():
      report[set] = {}
      for op, cmd in checks.items():
        if "{0}" in cmd: #needs pid
          cmd = cmd.format(pid)
        report[set][op] = "".join(ssh.run(cmd, timeout=2)["out"])

    if "slmctl" in ctl_path:
      for op, cmd in mds_ops.items():
        if "{0}" in cmd and "{1}" in cmd: #needs the paths
          cmd = cmd.format(ctl_path, self.runtime["build_dirs"]["ctl"])
        report[set][op] = "".join(ssh.run(cmd, timeout=2)["out"])
    elif "slictl" in ctl_path:
      for op, cmd in ion_ops.items():
        if "{0}" in cmd and "{1}" in cmd: #needs the paths
          cmd = cmd.format(ctl_path, self.runtime["build_dirs"]["ctl"])
        report[set][op] = "".join(ssh.run(cmd, timeout=2)["out"])

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
        output = self.check_status(ssh, ctl_path, pid)
      elif "bsd" in kernel:
        output = self.query_ctl_rusage(ssh, ctl_path)
      else:
        pass
      rusages["{0} [{1}]".format(host, pid)] = output

    return rusages

  def run_tests(self):
    """Run all tests from the tests directory and print results"""
    tset_results = []
    available_tests = [file for file in glob.glob(self.tests_dir + "/*") if os.access(file, os.X_OK)]
    results_file = path.join(self.runtime["build_dirs"]["base"], "results.json")
    f = open(results_file, "w")
    for test_file in available_tests:
      results = {"name": test_file.split(".")[-1]}
      print "Running", results["name"]
      f.write(results["name"] + "\n")

      start = time.time()
      results["retcode"] = subprocess.call([test_file, self.runtime_file])
      results["pass"] = results["retcode"] == 0
      results["elapsed"] = time.time() - start

      results["rusage"] = self.get_resource_usages()
      tset_results.append(results)

    f.write(json.dumps(tset_results))
    f.close()

  def cleanup(self):
    pass
    # TODO: we don't have permissions to delete these..
    #shutil.rmtree(self.modules_folder)

if __name__ == "__main__":
  #Ran from script
  TestHandler(sys.argv[1])

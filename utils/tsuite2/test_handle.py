import sys, json, pkgutil, time
import shutil

class TestHandler(object):
  """Object used to set up testing environment and passed along to tests for runtime information."""

  result_file = "results.json"

  def __init__(self, json_constants, modules_folder):
    self.runtime = json.loads(json_constants)
    self.modules_folder = modules_folder
    self.modules = load_all_modules_from_dir(modules_folder)
    self.run_tests()
    self.cleanup()

  def run_tests(self):
    """Run all tests from the tests directory and print results"""
    tset_results = {"tests": []}
    for module in self.modules:
      test = {"name": module.__name__}

      test["setup"]=module.setup()

      start = time.time()
      test["operate"]=module.operate()
      elapsed = time.time() - start
      test["operate"]["elapsed"] = elapsed

      test["cleanup"]=module.cleanup()

      tset_results["tests"].append(test)
      f = open(self.result_file, "w")
      f.write(json.dumps(tset_results))
      f.close()

  def cleanup(self):
    shutil.rmtree(self.modules_folder)


def load_all_modules_from_dir(dirname):
  modules = []
  for importer, package_name, _ in pkgutil.iter_modules([dirname]):
    full_package_name = '%s.%s' % (dirname, package_name)
    if full_package_name not in sys.modules:
      module = importer.find_module(package_name).load_module(full_package_name)
      modules.append(module)
  return modules

if __name__ == "__main__":
  #Ran from script
  TestHandler(sys.argv[1], "./modules")


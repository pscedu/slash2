import sys, json, pkgutil, time

class TestHandler(object):
  """Object used to set up testing environment and passed along to tests for runtime information."""

  def __init__(self, json_constants, modules):
    self.runtime = json.loads(json_constants)
    self.modules = modules
    self.run_tests()

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
    print json.dumps(tset_results)

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
  modules = load_all_modules_from_dir("modules")
  TestHandler(sys.argv[1], modules)


import sys, json

class TestHandler(object):
  """Object used to set up testing environment and passed along to tests for runtime information."""

  def __init__(self, json_constants):
    self.runtime = json.loads(json_constants)

  def run_tests(self):
    """Run all tests from the tests directory."""

if __name__ == "__main__":
  #Ran from script
  TestHandler(sys.argv[1])



class TestHandler(object):
  """Object used to set up testing environment and passed along to tests for runtime information."""

  cli_list = []

  def __init__(self):
    pass

  def add_cli(self, host, env = {}):
    """Add test endpoint.

    Args:
      host: Hostname of test site.
      env: dictionary of environment variables."""

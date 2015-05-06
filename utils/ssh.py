import paramiko, getpass, logging
import os, re

from time import sleep

log = logging.getLogger("slash2")

class SSH(object):
  """Helpful SSH abstractions for executing remote applications."""

  def __init__(self, user, host, password=None, port=22):
    """Initialize SSH object.

      Args:
        user: username.
        host: hostname of connection.
        password: user's password. If None, stdin will be prompted for pass.
                  If the user is using auth_keys, an empty string will work.
        port: port of destination's sshd.
      Raises: SSHException."""

    self.user = user
    self.host = host
    self.ssh = paramiko.SSHClient()
    self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    #Get password from stdin
    if password is None:
      password = getpass.getpass("{0}'s password: ".format(user))

    #Initialize connection
    try:
        self.ssh.connect(host, username=user, password=password, port=port)
    except Exception:
        raise paramiko.SSHException

  def list_screen_socks(self):
    """Return a list of open screen sockets."""

    socks = []
    sock_re = re.compile(r"\s+\d+\.(.+?)\s+.*?")
    sock_ls = self.run("screen -ls", quiet=True)["out"]
    for sock in sock_ls:
        sock_match = sock_re.match(sock)
        if sock_match:
          socks.append(sock_match.group(1))

    return socks

  def run_screen(self, cmd, sock_name, timeout=None):
    """Remotely execute a command in a screen session. If timeout is reached, screen will be renamed and kept open.

      Args:
        cmd: command string to be executed.
        sock_name: screen session's socket name.
        timeout: maximum execution time."""

    socks = self.list_screen_socks()

    #Sock already exists!
    exists = any([s.startswith(sock_name) for s in socks])
    if exists:
      return False

    log.debug("Launching screen: {0} at {1}".format(sock_name, self.host))

    #Sanitize newlines
    cmd = cmd.strip("\t\n ;")
    sane_cmd = ""
    for line in cmd.splitlines():
      sane_cmd += line.strip() + ";"

    cmd = sane_cmd

    #Debug -- log the cmds being run
    [log.debug(c) for c in cmd.split(";")]

    if timeout:
      timed_cmd = ""
      for line in cmd.split(";"):
        if len(line) > 0:
          timed_cmd += "timeout --signal=9 {0} {1}; ".format(timeout, line)
      cmd = timed_cmd

    #Add return code catch to each command
    cmd = cmd.replace(";", "; ck; ")

    #Wrap the command with a bash condition to rename and keep the screen session open
    shell_script = "ck(){{ c=$?; echo $c; if [[ $c != 0 ]]; then screen -S {0} -X zombie kr; if [[ $c == 137 ]]; then screen -S {0} -X sessionname {0}-timed; else screen -S {0} -X sessionname {0}-error; fi; exit; fi; }}".format(sock_name)

    cmd = "screen -S {0} -d -L -m $SHELL -c '{2}; {1}'"\
        .format(sock_name, cmd, shell_script)

    chan = self.ssh.get_transport().open_session()
    chan.exec_command(cmd)

    print cmd

    return True

  def wait_for_screen(self, sock_name, sleep_duration=3):
    """Blocks until a screen sock is removed or timesout.

    Args:
      sock_name: socket to be looking for.
      sleep_duration: time to sleep inbetween checks.
    Returns:
      dict {
        timedout: true/false
        finished: true/false
        errored: error code
      }"""

    result = {
      "timedout": False,
      "finished": False,
      "errored": False
    }

    #initial rest
    sleep(sleep_duration)

    while True:
      alive = False
      for sock in self.list_screen_socks():
        if sock == sock_name:
          alive = True
          break

        if sock == sock_name + "-timed":
          #Screen timed out
          result["timedout"] = True
          return result
        elif sock == sock_name + "-error":
          result["errored"] = True
          return result

      #If it is still running, sleep for a second
      if alive: sleep(sleep_duration)
      else:
        result["finished"] = True
        return result

  def run(self, cmd, timeout=None, quiet=False):
    """Remotely execute a command.

      Args:
        cmd: command string to be executed.
        timeout: maximum execution time.
      Returns:
        dict {
          out: stdout.
          err: stderr.
          exit: exit code of the cmd.
                timeout returns 137.
        }"""

    #Sanitize newlines
    cmd = cmd.replace("\n", ";")

    #Debug -- log the cmds being run
    if not quiet:
      [log.debug(c) for c in cmd.split(";")]

    if timeout:
      cmd = "timeout --signal=9 {0} {1}".format(timeout, cmd)

    chan = self.ssh.get_transport().open_session()
    chan.exec_command(cmd)

    result = {
      "out" : list(chan.makefile("rb")),
      "err" : list(chan.makefile_stderr("rb")),
      "exit": chan.recv_exit_status()
    }

    return result

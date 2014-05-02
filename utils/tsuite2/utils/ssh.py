import paramiko, getpass, logging
import os, re, errno
from os import path

from time import sleep

log = logging.getLogger('ssh')
logging.getLogger("paramiko").setLevel(logging.WARNING)

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
    #    if password is None:
    #     password = getpass.getpass("{0}'s password: ".format(user))

    #Initialize connection
    try:
        self.ssh.connect(host, username=user, password=password, port=port)
    except Exception:
        raise paramiko.SSHException

    self.sftp = self.ssh.open_sftp()

  def close(self):
    self.sftp.close()
    self.ssh.close()

  def recursive_copy(self, src, dst):
    """Recursively copy local path to remote path. Not elevated.

    Args:
      src: local path.
      dst: remote path."""

    self.make_dirs(dst)
    src = src.rstrip(os.sep)
    dst = dst.rstrip(os.sep)
    for root, dirs, files in os.walk(src):

      dst_root = dst + root[len(src):]

      for d in dirs:
        remote_path = os.path.join(dst_root, d)
        self.make_dirs(remote_path)

      for f in files:
        path = os.path.join(root, f)
        remote_path = os.path.join(dst_root, f)
        self.copy_file(path, remote_path)

  def copy_file(self, src, dst, elevated=False):
    """Copy local file to remote server. Will not be elevated. :(

    Args:
      src: path to local file.
      dst: path to copy to on remote server.
      elevated: attempt to hackily escalate privileges
    Returns:
      True if it copied successfully, False if the src file does not exist.
      Can also throw an IOException"""

    try:
      if os.path.isfile(src):
        if elevated:
          temp_dst = path.join("/tmp", path.basename(dst))
          self.copy_file(src, temp_dst)
          self.run("sudo mv {0} {1}".format(temp_dst, dst))
          #Seems unnecessary
          #log.debug("Copied file {0} to {1} on {2} with elevated privileges".format(path.basename(src), dst, self.host))
        else:
          s = open(src, "rb")
          contents = s.read()
          s.close()
          f = self.sftp.open(dst, "wb")
          f.write(contents)
          f.close()

          #log.debug("Copied file {0} to {1} on {2}".format(path.basename(src), dst, self.host))
        return True
      else:
        log.error(src + " does not exist locally!")
        return False
    except IOError, e:
      log.error("Cannot copy file {0} to {1} on {2}!".format(src, dst, self.host))
      log.error(str(e))

  def pull_file(self, rmt, local):
    """Download remote file. Not elevated.

    Args:
      rmt: path to file on the remote machine.
      local: path to store remote file on local machine."""

    r = self.sftp.open(rmt, "rb")
    contents = r.read()
    r.close()
    l = open(local, "wb")
    l.write(contents)
    l.close()

    return True

  def make_dirs(self, dirs_path, escalate=False):
    """Create remote directories.

    Args:
      dirs_path: directory path.
      force: attempt to elevate and create"""

    #log.debug("Making directory {0} on {1}.".format(dirs_path, self.host))
    levels = dirs_path.split(os.sep)
    for level in range(1, len(levels)):
      try:
        path = os.sep.join(levels[:level+1])
        if escalate:
          if self.run("sudo mkdir {0}".format(path), quiet=True)['err'] == []:
            self.run("sudo chmod 0777 {0}".format(path))
            #log.debug("Created directory {0} on {1} with escalated priveleges.".format(path, self.host))
        else:
          self.sftp.mkdir(path)
      except IOError as error:
        if error.errno != None: #directory doesn't exist
          log.error("Could not make directory {0} on {1}.".format(path, self.host))


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

  def kill_screens(self, sock_name_prefix, exact_sock=False, quiet=False):
    """Kills a remote sock.

    Args:
      sock_name_prefix: prefix of any socks to kill.
      exact_sock: Consider the prefix to be the exact name.
      quiet: Silent output.
    Returns: number of socks killed."""

    sock_list = self.list_screen_socks()
    log.debug("Quitting {0}screen sessions: {1}".format("exact " if exact_sock else "", sock_name_prefix))

    check = lambda sock: sock == sock_name_prefix if exact_sock else\
            lambda sock: sock.startswith(sock_name_prefix)

    targeted_socks = filter(check, sock_list)
    for sock in targeted_socks:
      self.run("screen -X -S {0} quit".format(sock), quiet)

    return len(targeted_socks)

  def run_screen(self, cmd, sock_name, timeout=None, quiet=False):
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

    if quiet:
      #Debug -- log the cmds being run
      [log.debug(c) for c in cmd.split(";")]

    if timeout:
      timed_cmd = ""
      for line in cmd.split(";"):
        if len(line) > 0:
          timed_cmd += "sudo timeout --signal=9 {0} {1}; ".format(timeout, line)
      cmd = timed_cmd

    #Add return code catch to each command
    cmd = cmd.replace(";", "; ck; ")

    #Wrap the command with a bash condition to rename and keep the screen session open
    shell_script = "ck(){{ c=$?; echo $c; if [[ $c != 0 ]]; then screen -S {0} -X zombie kr; if [[ $c == 137 ]]; then screen -S {0} -X sessionname {0}-timed; else screen -S {0} -X sessionname {0}-error; fi; exit; fi; }}".format(sock_name)

    cmd = "screen -S {0} -d -L -m $SHELL -c '{2}; {1}'"\
        .format(sock_name, cmd, shell_script)

    chan = self.ssh.get_transport().open_session()

    chan.exec_command(cmd)
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
      [log.debug("{0}@{1}:~/$ {2}".format(self.user, self.host, c)) for c in cmd.split(";")]

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

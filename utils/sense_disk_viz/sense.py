#! /usr/bin/python2.7
import argparse, os, sys, re, json, logging, time

log = logging.getLogger("slash2")

def find(drives, key, value, host = None):
  if host:
    drives = drives[host]

  for drive in drives:
    if key in drive.keys():
      if key == "scsi_id": #special case ; ignore first character
        if drive[key][1:] == value[1:]:
          return drive
      else: 
        if drive[key] == value:
          return drive
  return None

def parse_willie(willie_out):

  if os.path.isfile(willie_out):
    willie_out = open(willie_out, "r")
  else:
    log.critical("Willie -> %s does not exist.", willie_out)
    sys.exit(1)

  willie_drives = {}

  current_host = None

  for line in willie_out.readlines():
    line = line.rstrip()

    new_host = re.match(r"^Welcome.+?on (.+)$", line)
    if new_host:
      if current_host:
        log.debug("Willie -> Found %s drives on %s", len(willie_drives[current_host]), current_host)
      current_host = new_host.group(1)
      willie_drives[current_host] = []

    if current_host:
      disk_regex = r"^type=disk,subtype=SATA,serial=(.+?),.+?sd0=(.+?),.+?scsi_id0=(.+?),.+?backplane0=(.+?),slot0=(.+?)"
      new_drive = re.match(disk_regex, line)
      if new_drive:
        d = {}
        d["serial"], d["sd"], d["scsi_id"], d["backplane"], d["slot"] = new_drive.groups()
        willie_drives[current_host].append(d)

  log.debug("Willie -> Found %s drives on %s", len(willie_drives[current_host]), current_host)
  return willie_drives

if __name__ == "__main__":

  parser = argparse.ArgumentParser()

  parser.add_argument("-f", "--file-inputs", required=True, action="append", help="dmesg inputs to be read. specify -f host:file Ex. sense3.psc.edu:dmesg.log")
  parser.add_argument("-v", "--verbose", action="count", help="Increase verbosity", default=0)
  parser.add_argument("-o", "--out", default=sys.stdout, help="Specify output. If output already exists, data will be combined. Defaults to stdout")
  parser.add_argument("-w", "--willie-file", help="Specify willie output.", required=True)

  args = parser.parse_args()

  level = [logging.WARNING, logging.INFO, logging.DEBUG][2 if args.verbose > 2 else args.verbose]
  log.setLevel(level)

  #Setup stream log (console)
  fmt_string = "%(asctime)s <%(levelname)s> %(message)s"\

  ch = logging.StreamHandler()
  ch.setLevel(level)
  ch.setFormatter(logging.Formatter(fmt_string))

  log.addHandler(ch)

  willie = parse_willie(args.willie_file)

  docErr = {}

  if args.out != sys.stdout:
    log.debug("Opening %s for writting.", args.out)
    args.out = open(args.out, "w")

  errRegex = {
      "termination": re.compile(r"^mps\d+: \((.+?)\) terminated"),
      "scsi_status": re.compile(r"^\((.+?):mps\d+:(.+?)\): (READ|WRITE)\(\d+\)"),
      "scsi_timeout": re.compile(r"^\((.+?):mps\d+:(.+?)\): SCSI command timeout .+? SMID (\d+)")
      #Going to need to actually look these up somehow...
      #mps0: mpssas_complete_tm_request: sending deferred task management request for handle 0x100 SMID 189
      #mps0: mpssas_abort_complete: abort request on handle 0x100 SMID 189 complete
  }

  host_list = {} 
  for i in xrange(0, 8):
    disk_dict = {}
    letter = chr(ord('a') + i / 2)
    if i % 2 == 0:
      side = "front"
    else:
      side = "rear"
    for j in xrange(0, 11):
      disk_dict["sdisk{}{}-{}".format(letter, j,  side)] = 0

    host_list["sense{}".format(i)] = disk_dict

  for host, file_path in [af.split(":") for af in args.file_inputs]:
    if not os.path.isfile(file_path):
      log.critical("%s is not a file!", file_path)
      sys.exit(1)
    if not host in willie:
      log.critical("%s was not listed in the willie output!", host)
      log.critical("hosts listed are: %s", ", ".join(willie.keys()))
      sys.exit(1)
    f = open(file_path, "r")
    log.debug("Looking for disk errors (%s from %s)", file_path, host)
    for line in f.readlines():
      line = line.rstrip()
      result = dict([(name, reg.match(line)) for name, reg in errRegex.items()])

      if result["termination"]:
        (scsi_id,) = result["termination"].groups()
        error_type = "termination"

      elif result["scsi_status"]:
        (sd, scsi_id, op) = result["scsi_status"].groups()
        error_type = "scsi_status"

      elif result["scsi_timeout"]:
        (sd, scsi_id, smid) = result["scsi_timeout"].groups()
        error_type = "scsi_timeout"
      else:
        continue

      error_dict = find(willie[host], "scsi_id", scsi_id)
      if error_dict:
        error_dict["error_type"] = error_type
        host_list[host[:6]][error_dict["backplane"]] += 1

      f.close()

  args.out.write(json.dumps(host_list))


import gdb

def check_resource_usage(event):
  logfile = open("%base%/mds_resource_usage", "a")
  logfile.write(gdb.execute("info proc status", to_string=True))
  logfile.close()
  gdb.execute("c")

gdb.execute("set confirm off")
gdb.execute("set height 0")
gdb.execute("handle SIGUSR1 ignore")
gdb.execute("run -S %base%/ctl/slashd.%h.sock -f %base%/slash.conf -D %datadir% -p %zpool_cache% %zpool_name%")

gdb.events.stop.connect(check_resource_usage)

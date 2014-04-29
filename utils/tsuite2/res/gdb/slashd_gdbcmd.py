import gdb

query_num = 0
def check_resource_usage(event):
    global query_num
    if isinstance(event, gdb.SignalEvent) and event.stop_signal == "SIGUSR1":
        logfile = open("%base%/rusage", "a")
        logfile.write("Query %d:\n" % query_num)
        query_num += 1

        rusage = gdb.execute("info proc status", to_string=True)
        fields = ["VmPeak", "VmSize", "Threads",]
        for line in rusage.split("\n"):
          for field in fields:
            if field in line:
              logfile.write(line)
              continue
        logfile.write("%s\n" % ("-"*10))
        logfile.close()

        gdb.execute("c")

gdb.execute("set confirm off")
gdb.execute("set height 0")
gdb.execute("handle SIGUSR1 ignore")
gdb.events.stop.connect(check_resource_usage)
gdb.execute("run -S %base%/ctl/slashd.%h.sock -f %base%/slash.conf -D %datadir% -p %zpool_cache% %zpool_name%")


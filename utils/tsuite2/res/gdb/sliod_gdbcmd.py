import gdb

query_num = 0
def check_resource_usage(event):
    if isinstance(event, gdb.SignalEvent) and event.stop_signal == "SIGUSR1":
        logfile = open("%base%/mds_resource_usage", "a")
        logfile.write("Query %d:\n" % query_num)
        query_num += 1

        rusage = gdb.execute("info proc status", to_string=True)
        fields = ["VmPeak", "VmSize", "Threads",]
        [[logfile.write(line) if field in line for field in fields] for line in rusage.split("\n")]
        logfile.write("%s\n" % ("-"*10))
        logfile.close()

        gdb.execute("c")

gdb.execute("set confirm off")
gdb.execute("set height 0")
gdb.execute("handle SIGUSR1 ignore")
gdb.execute("run -S %base%/ctl/sliod.%h.sock -f %base%/slash.conf -D %datadir% %prefmds%")

gdb.events.stop.connect(check_resource_usage)

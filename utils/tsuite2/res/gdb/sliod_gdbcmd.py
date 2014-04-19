import gdb

def check_resource_usage(event):
    if isinstance(event, gdb.SignalEvent) and event.stop_signal == "SIGUSR1":
        logfile = open("%base%/mds_resource_usage", "a")
        logfile.write(gdb.execute("info proc status", to_string=True))
        logfile.close()
        gdb.execute("c")

gdb.execute("set confirm off")
gdb.execute("set height 0")
gdb.execute("handle SIGUSR1 ignore")
gdb.execute("run -S %base%/ctl/sliod.%h.sock -f %base%/slash.conf -D %datadir% %prefmds%")

gdb.events.stop.connect(check_resource_usage)

import gdb

def resource_handler(event):
  print "yay signal: {0}".format(event.stop_signal)

gdb.events.stop.connect(resource_handler)

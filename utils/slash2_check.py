#!/usr/bin/python
# $Id$
#
# File:         slash2_check.py
# Description:  report status of SLASH2 functions on on clients and servers
# History:	2011 by jasons@psc.edu
#
# $Id: slash2_check.py,v 1.20 2012/09/13 19:36:28 jasons Exp $

import threading, socket, syslog, re, os, signal
from optparse import OptionParser
from subprocess import Popen,PIPE
from fnmatch import fnmatch
from sys import argv,exit
from time import time
from boundcmd import boundcmd

# Add compatibility for old(2.4/xfer-admin) python versions
try:
	import hashlib
	oldschool = False
except ImportError:
	import md5
	oldschool = True

################### Global variables ##############################
# hostname of Nagios server to receive messages
nagios_server="saint-100.psc.edu"

# This is a dummy return value used to signify a thread was killed
termFlag = -10000

# Per-host settings
# XXX should be read in from a configuration file.
hostcfg = {
	"illusion1":	{ "type": "mds" },
	"illusion2":	{ "type": "mds" },
	"burton":	{ "type": "mds" },
	"dennis":	{ "type": "mds" },

	"sense0":	{ "npools": 8, "path": "/arc_sliod/%i" },
	"sense1":	{ "npools": 7, "path": "/arc_sliod/%i" },
	"sense2":	{ "npools": 8, "path": "/arc_sliod/%i" },
	"sense3":	{ "npools": 7, "path": "/arc_sliod/%i" },
	"sense4":	{ "npools": 8, "path": "/arc_sliod/%i" },
	"sense5":	{ "npools": 7, "path": "/arc_sliod/%i" },
	"sense51":	{ "npools": 7, "path": "/sense51_pool%i" },

	"dxcsbb01":	{ "npools": 8, "path": "/sbb1_pool%i" },
	"dxcsbb02":	{ "npools": 8, "path": "/sbb2_pool%i" },
	"dxcsbb03":	{ "npools": 8, "path": "/sbb3_pool%i" },
	"dxcsbb04":	{ "npools": 8, "path": "/sbb4_pool%i" },
	"dxcsbb05":	{ "npools": 8, "path": "/sbb5_pool%i" },
}

testdir = '/arc/users/root'
lockfile = '/var/run/zpools_busy'

useExternalTest = True		# Use external dd rather than internal thread

################### Object definitions #####################
#Define a lookup table for Nagios return code values
class nagios:
	ok = '0'
	warn = '1'
	critical = '2'
	unknown = '3'

#Define a lookup table for test types
class tests:
	slash = 0
	zpool = 1

#Define a class to summary the zpool status
class zpool:
	name = None
	state = None
	action = ''
	config = None
	errors = ''

def dprint(msg):
	global options
	if options.pdebug:
		print msg

def readTest():
	global options
	tout= ''
	terr= ''
	rc = 1
	dprint('DEBUG Read Test thread started')
	fname = testdir + '/slash2_check.in'
	try:
		fd = os.open(fname,os.O_NONBLOCK | os.O_RDONLY)
		if fd == None :
			return ( 2, '', "open(%s) would block!" % fname)
		data = fd.read()
		if data.strip() == 'data_from_file':
			rc = 0
			tout = 'Small file read and contents verified.'
		else :
			terr = "Small file read failed!"
	except:
		terr= 'failed to open ' + fname
	return (rc, tout, terr)

def writeTest():
	global options
	tout= ''
	terr= ''
	rc = 1

	fname = testdir + '/slash2_check.out-' + hn
	dprint('DEBUG Write Test thread started')
	#TODO : write a unique string each time... perhaps a date
	try:
		fd = os.open(fname,os.O_NONBLOCK | os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
		if fd == None :
			return ( 2, '', "open(%s) would block!" % fname)
		# TODO: write our more unique info (date/time???)
		fd.write('foo-bar')
		rc = 0
		tout = 'Smalle file write succeeded'
		# TODO: consider reading back to verify
		fd.close()
	except:
		terr = 'failed to open ' + fname
	return (rc, tout, terr)

def sliodPoke(backfspath):
	global options
	tout= ''
	terr= ''
	rc = 2

	stestdir = '%s/.selftest/slash2_check-%f' % (backfspath,time())
	# Attempt to create a directory and write to a file in that directory
	try :
		blk_size = 579
		num_blks = 1024
		terr = 'an exception occurred trying to create test directory %s. ' % stestdir
		os.mkdir(stestdir)
		fname = stestdir + '/datafile'
		zeros = "\x00" * blk_size
		if not oldschool :
			zmd5=hashlib.md5()
		else :
			zmd5 = md5.new()

		# Write a bunch of data to a file and calculate the md5 of the data (while writing)
		dprint("DEBUG: attempting to write to %s" % fname)
		terr = 'an exception occurred trying to create test file %s. ' % fname
		f = open(fname,'wb')
		for i in range(num_blks) :
			f.write(zeros)
			zmd5.update(zeros)
		f.close()

		# Read back the file data and calculate the md5 sum
		dprint("DEBUG: attempting to read to %s" % fname)
		terr = 'an exception occurred trying to read test file %s. ' % fname
		f = open(fname,'rb')
		if not oldschool :
			fmd5 = hashlib.md5()
		else :
			fmd5 = md5.new()
		while True:
			buf = f.read(8096)
			if not buf:
				break
			fmd5.update(buf)
		f.close()

		# compare the md5 sums and report the outcome
		terr = "an exception occurred trying to comare the md5 sums. "
		dprint("DEBUG: output md5 = %s, input md5 = %s" % (zmd5.hexdigest(),fmd5.hexdigest()))
		if zmd5.hexdigest() == fmd5.hexdigest() :
			tout = 'md5sum matched'
		else :
			tout = 'md5sum did not match'
		terr = 'an exception occurred when trying to cleanup'
		os.unlink(fname)
		os.rmdir(stestdir)
		rc = 0
		terr = None
	except OSError, e :
		terr = "%sOSError %i : %s" % (terr,e.errno,e.strerror)
		rc = 1
	except :
		rc = 1
	return (rc, tout, terr)

####### Function to perform hostname munging specific to PSC conventions:
def scrubNames(line):

	if line is not None:
		line = line.strip()

		# Merge the multiple lines of Tahini into one (before dropping '99')
		line = line.replace('-99o','')

		# Drop the -99 from multi-homed hosts
		line = line.replace('-99','')

		# Replace local address of blacklight login nodes with teragrid one
		line = line.replace('bl-login1.psc.edu','tg-login1.blacklight.psc.teragrid.org')
		line = line.replace('bl-login2.psc.edu','tg-login2.blacklight.psc.teragrid.org')
	return line

####### Function to evaluate the output of a SLASH2 command and inform Nagios
def slashEval(thost,tname,cmd,returncode,textout,texterrors):
	clients=[]
	clict=0

	# If the command returned successfully, check that there are many lines
	if returncode == 0:
		srvct=0
		mode=0
		clients=[]

		for line in textout.split('\n') :
			#Skip the link if we don't understand it
			if len(line.strip()) < 7 :
				continue
			#If not counting servers yet, see if we should start
			if mode == 0 :
				if line[0:7] == 'PSCARCH' :
					mode = 1
					continue
			#If counting servers, count until we fine the 'clients' line
			elif mode == 1 :
				#If we hit the 'clients' marker, switch modes
				if line[0:7] == 'clients' :
					mode = 2
					continue
				#Count the line if it looks like a server
				fields = line.split()
				if len(fields) == 7 :
					srvct = srvct + 1
					#Check the status of the metadata server
##                                      if fields[3].strip() == 'mds' :
##                                              if fields[4][0] == 'C' and fields[4][2] == 'M'

			#If counting servers, do so until we hit a line we don't understand
			elif mode == 2 :
				fields = line.split()
				if len(fields) == 5 :
					client = scrubNames(fields[0])
					clients.append(client)
				else :
					break

		clict = len(clients)
		# If lots of sliods were seen, consider it successful
		if srvct > 10:
			rval = nagios.ok
			rinfo = "%s listed %i I/O servers and %i clients" % (cmd,srvct,clict)
		# If few were seen, consider it suspecious
		else :
			rval = nagios.warn
			rinfo = "%s listed ONLY %i I/O servers and %i clients" % (cmd,srvct,clict)

	# If the command had to be terminated--consider this an error
	elif returncode == termFlag :
		rval = nagios.critical
		rinfo = 'command killed due to TIMEOUT(%s)' % texterrors

	# If the command didn't complete correctly, consider it ambiguous
	else :
		rval = nagios.unknown
		rinfo = cmd + " had return code of %i (%s)" % (returncode,texterrors)

	pinfo = ''
	if clict  > 0 and not options.dontSend :
		pinfo = "clients: " + ",".join(clients)
		dprint("reporting clients: %s" % clients)
		try :
			syslog.openlog('slash2_client',0,syslog.LOG_DAEMON)
			for client in clients :
				syslog.syslog(syslog.LOG_INFO, client + " was seen as a SLASH2 client of " + hn)
				break
			syslog.closelog()
		except :
			print "ERROR: unable to send slash2_client message to log server"

	dprint('DEBUG: slashEval returning: ' + rval + ":" + rinfo + ":" + pinfo)
	nsca_queue(thost,tname,rval,rinfo,pinfo)

def versionEval(thost,tname,cmd,returncode,textout,texterrors):
	version = 'UNKNOWN'

	##dprint("versionEval checking %i,'%s' for version" % (returncode,textout))

	if returncode == 0 :
		for line in textout.split('\n') :
			if line.find('version') == 0 :
				fields = line.split()
				if len(fields) == 2 :
					version = fields[1]
					break

	dprint("DEBUG: reporting %s/%s running version %s of SLASH2 code" % (thost,tname,version))

	# Log a message with the SLASH2 version number
	try :
		syslog.openlog('slash2_version',0,syslog.LOG_DAEMON)
		syslog.syslog(syslog.LOG_INFO, hn + "/" + tname + " running version " + \
				version + " of SLASH2 software")
		syslog.closelog()
	except :
		print "ERROR: unable to send slash2_version message to log server"

###### Function to evaluate output of a zpool test and inform Nagios
def zpoolEval(thost,tname,cmd,returncode,textout,texterrors):

	# Note. this routine assumes fairly rigid output and zpool naming; if either
	# change, this routine will require tuning

	# If the command returned successfully, interpret the output
	pastheaders=False
	pinfoa = []
	rinfoa = []
	pinfo = ''
	rinfo = ''
	if returncode == 0:
		zstate = ''
		poolname = ''
		dbad = 0
		dgood = 0
		dsilv = 0
		dtotal = 0
		perm = None
		for line in textout.split('\n'):

			#pick off a little info from the header
			if not pastheaders :

				#if the line has a label, record it
				if fnmatch(line,'*pool:*'):
					poolname = line.split()[1].strip()
					continue

				#if the line has a top level state, evaluate it
				elif fnmatch(line,'*state:*') :
					zstate = line.split(':')[1].strip()
					rinfoa.append("%s=%s" % (poolname,zstate))
					if zstate == 'ONLINE':
						rval = nagios.ok
						continue
					elif zstate == 'FAULTED' :
						rval = nagios.critical
						continue
					else :
						#default to warning; may be raised by evaluation of details
						rval = nagios.warn
						continue

				elif fnmatch(line,'*resilvered,*%*done') :
					fields = line.split()
					if len(fields) > 2 :
						pinfoa.append("%s done resilvering, " % fields[2])

				elif fnmatch(line,'*NAME*STATE*READ*WRITE*CKSUM*') :

					# If we didn't understand the overview, give up before trying details
					if zstate == '' :
						rval = nagios.unknown
						rinfoa.append('could not parse zpool status output')
						break
					dprint("DEBUG: made it past the zpool headers")
					pastheaders = True

			# dig through the details to see how bad things are
			else :
				if len(line.strip()) == 0:
					continue
				fields = line.split()
				key = fields[0].strip()

				dprint("DEBUG: evaluating line:" + str(fields))

				if (perm != None) and (key != 'pool:') :
					dprint("DEBUG: adding file to perm '%s'" % line.strip())
					perm.append(line.strip())
					dprint("DEBUG: perm is now '%s'" % " ".join(perm))
					continue

				# If this is the top level config info, skip it (we already have it)
				if fnmatch(key,'*_pool*') or fnmatch(key,'*mirror*'):
					continue

				# If we're starting a new RAIDz within the zpool; reset counters
				elif fnmatch(key,'*raidz*') :
					# If bad disks were found in previous raidz set, evaluate how many
					if dbad != 0:
						if dbad > 1 or dsilv > 1:
							rval = nagios.critical
						rinfoa.append("%s had %i/%i bad drives (%i OK, %i resilvering) , " % (poolname,dbad, dtotal, dgood, dsilv))
						dbad = 0
					poolname = key
					dgood = 0
					dsilv = 0
					dtotal = 0
					continue
				elif (key == "errors:") :
					dprint("DEBUG: detected errors '%s'" % line)
					if "Permanent errors" in line :
						dprint("DEBUG: Permenant errors detected")
						perm = []
						continue
					# TODO: consider handling other errors here !!!
					else :
						break

				# If we made it this far, it "must" be a disk within a raidz
				dtotal += 1

				if fields[1].strip() != 'ONLINE' :
					#skip the drive if it's being replaced as the old disk will be detected on a different line
					if 'replacing' in line :
						dtotal -= 1
					else :
					    dbad += 1
					    dprint("found %i bad disk (%s) of %s" %(dbad,key,poolname))
				elif 'resilvering' in line:
					dsilv += 1
					dprint("found %i disk resilvering (%s) of %s" %(dsilv,key,poolname))
				else :
					dgood += 1
		if perm != None :
			pinfoa.append("%s perm. errors in %s" % (poolname," ".join(perm)))

		if len(rinfoa) > 0 : rinfo=",".join(rinfoa)
		else: rinfo=''
		if len(pinfoa) > 0 : pinfo=",".join(pinfoa)
		else: pinfo=''

	# If the command had to be terminated--consider this an error
	elif returncode == termFlag :
		rval = nagios.critical
		rinfo = 'command killed due to TIMEOUT (%s)' % texterrors
	else :
		rval = nagios.unknown
		rinfo = cmd + " had return code of %i (%s)" % (returncode,texterrors)

	try :
		i = tname[len(tname)-1]
		apath = '/arc_sliod/' + str(i)
		if os.path.ismount(apath) :
			stat = os.statvfs(apath)
			used  = "%i%% in use" % ((stat.f_blocks - stat.f_bavail) *100/stat.f_blocks)
			if len(pinfo) > 0 : pinfo = "%s,%s" % (pinfo,used)
			else : pinfo = used
	except:
		pass
	dprint('DEBUG: zpoolEval returning: %s:%s:%s' % (rval,rinfo,pinfo))
	nsca_queue(thost,tname,rval,rinfo,pinfo)

####################### Helper functions #######################################
# function to queue data to be sent to Nagios in bulk:
def nsca_queue(testHost,service,rval,info = '', perf_info = None):
	global nsca_data

	info = info.replace('\n',';')

	# Assemble the delimited string that NSCA should send
	# NOTE: for some reason the 'status' and 'performance' data should be
	# separated by a '|' character rather than a tab!
	if perf_info == None or perf_info == '':
		payload="%s\t%s\t%s\t%s\n" % (testHost,service,rval,info)
		payload=testHost + '\t' + service + '\t' + rval + '\t' + info  + '\n'
	else :
		payload="%s\t%s\t%s\t%s|%s\n" % (testHost,service,rval,info,perf_info)

	dprint("DEBUG: queuing up payload of:  " + payload)
	nsca_data = nsca_data + payload

# function to actually send text data to NSCA via nsca_send utility
def nsca_send_raw(payload):

	if options.dontSend :
		print "DEBUG: skipping send of::\n" + payload
	else :
		print "Sending update to Nagios::\n" + payload
		# Open the send_nsca program and send the message
		reporter=Popen('send_nsca -H ' + nagios_server + ' -c /etc/send_nsca.cfg',stdin=PIPE,stdout=PIPE,stderr=PIPE,shell=True)
		(r_out,r_err)=reporter.communicate(payload)
		dprint("DEBUG: NSCA output: " + r_out)
		if len(r_err) > 0 : print "DEBUG: NSCA error: " + r_err
		elif r_out.find("error") >= 0 or r_out.find("error") >= 0 :
			print "NSCA reported error: " + r_out + ";" + r_err
		else :
			print "NSCA_SEND reported: " + r_out

# function to send data to Nagios immediately/unbuffered
def nsca_send(testHost,service,rval,info = '', perf_info = None):

	# Assemble the delimited string that NSCA should send
	# NOTE: for some reason the 'status' and 'performance' data should be
	# separated by a '|' character rather than a tab!
	if perf_info == None or perf_info == '':
		payload=testHost + '\t' + service + '\t' + rval + '\t' + info  + '\n'
	else :
		payload=testHost + '\t' + service + '\t' + rval + '\t' + info  + '|' + perf_info + '\n'

	nsca_send_raw(payload)

# function to add a directory to the PATH environment variable
def path_munge(*paths):
	global options

	# Add each directory to the path if it exits and isn't already there
	for p in paths :
		if not os.path.exists(p) :
			dprint("DEBUG: path_munge called on non-existent directory:  " + p)
			continue
		if p in os.getenv('PATH').split(os.pathsep) :
			dprint("DEBUG: path_munge : '%s' already in path" % p)
			continue
		# Finally, add the new directory to the path
		os.environ['PATH'] = os.pathsep.join([os.getenv('PATH'), p])
		dprint("DEBUG: added '%s' to path" % p)

def test_client(prefix=''):
	global options

	dprint("running SLASH2 Client tests")

	# Test the SLASH2 client connections directly
	test_cmd='msctl -sconn'
	tname = prefix + 'msctl'
	bc = boundcmd(cmd=test_cmd,maxtries=2,timeout=10,verbose=options.pdebug)
	(rc,tout,terr)=bc.run()
	slashEval(hn,tname,test_cmd,rc,tout,terr)

	# Record the SLASH2 version number
	test_cmd = 'msctl -p version'
	bc = boundcmd(cmd=test_cmd,maxtries=2,timeout=10,verbose=options.pdebug)
	(rc,tout,terr)=bc.run()
	versionEval(hn,tname,test_cmd,rc,tout,terr)

	if not options.quick :
		# Test reading of a file from /arc
		if useExternalTest :
			test_cmd="dd if=%s/slash2_check.in of=/dev/null" % testdir
			dprint("trying read command '%s'" % test_cmd)
			bc = boundcmd(cmd=test_cmd,maxtries=2,timeout=5,verbose=options.pdebug)
		else :
			bc = boundcmd(func=readTest,maxtries=2,timeout=5,verbose=options.pdebug)
		(rc,tout,terr)=bc.run()
		tname = prefix + 'arc'
		if rc != 0 :
			bc = boundcmd(cmd='msctl -Hr %s/slash2_check.in' %
			    testdir,maxtries=1,timeout=3,verbose=options.pdebug)
			(rc,bmaptab,bmaperr)=bc.run()
			nsca_queue(hn,tname,nagios.critical,'/arc READ test failed; cmd: ' + \
			    test_cmd + '; map: ' + bmaptab + '; err: ' + terr)
			return

		# Test writing a file from /arc
		fn='%s/slash2_check.out-%s' % (testdir,hn)

		if useExternalTest :
			test_cmd="dd if=/dev/zero of=%s bs=1M count=1" % fn
			dprint("trying write command '%s'" % test_cmd)
			bc = boundcmd(cmd=test_cmd,maxtries=2,timeout=5,verbose=options.pdebug)
		else :
			bc = boundcmd(func=writeTest,maxtries=2,timeout=5,verbose=options.pdebug)
		(rc,tout,terr)=bc.run()
		if rc != 0 :
			dprint("DEBUG: /arc write failed (%s);; %i:%s" % (test_cmd,rc,terr))
			nsca_queue(hn,tname,nagios.critical,'/arc WRITE test failed; cmd: ' + \
			    test_cmd + '; err: ' + terr)
		else :
			nsca_queue(hn,tname,nagios.ok,'/arc READ & WRITE tests passed')

def test_ioserver():
	global options

	dprint("running SLASH2 I/O server tests")

	#Abort if lock file is present as host is still mounting pools
	if os.path.exists(lockfile):
		dprint("DEBUG: lockfile exists; skipping zpool/sliod tests")
		return

	# Test the expected number of sliods on each node
	i = 0
	cfg = hostcfg[hn]

	while ( i < cfg["npools"] ):

		# Check zpool i
		test_cmd='zpool status -v %s_pool%i' % (hn,i)
		bc = boundcmd(cmd=test_cmd,maxtries=2,timeout=5,verbose=options.pdebug)
		(rc,tout,terr)=bc.run()
		zpoolEval(hn,'zpool%i' % i,test_cmd,rc,tout,terr)

		tname = 'sliod%i' % i
		rc = 0

		backfspath = cfg["path"] % i

		if not options.quick:
			# Perform functional test of sliod
			bc = boundcmd(sliodPoke,args=backfspath,maxtries=2,timeout=10,verbose=options.pdebug)
			(rc,tout,terr) = bc.run()

		# If the functional test succeeded, then gather list of connections via slictl
		if rc == 0 :
			test_cmd="slictl%i -sconn" % i
			bc = boundcmd(cmd=test_cmd,maxtries=2,timeout=5,verbose=options.pdebug)
			(rc,tout,terr)=bc.run()
			slashEval(hn,tname,test_cmd,rc,tout,terr)

			# Record the SLASH2 version number
			test_cmd = 'slictl%i -p version' % i
			bc = boundcmd(cmd=test_cmd,maxtries=2,timeout=5,verbose=options.pdebug)
			(rc,tout,terr)=bc.run()
			versionEval(hn,tname,test_cmd,rc,tout,terr)

		# If the functional test failed, tell Nagios
		else :
			dprint("DEBUG: /arc write failed (%s);; %i:%s" % (test_cmd,rc,terr))
			nsca_queue(hn,tname,nagios.critical,'sliod zpool/filesystem FAILED functional test : ' + terr)

		# increment the counter and loop again
		i = i+1

def test_mdserver():

	dprint("running SLASH2 Metadata server tests")

	vhn = 'illusion-mds'

	#Only run the test on the active node--as signified by /local being mounted

	test_cmd='slmctl -sconn'
	bc =  boundcmd(cmd=test_cmd,maxtries=2,timeout=10,verbose=options.pdebug)
	(rc,tout,terr) = bc.run()

	# Rewrite hostnames of multi-homed hosts
	mod_tout = scrubNames(tout)

	slashEval(vhn,'slmctl',test_cmd,rc,mod_tout,terr)

	# Record the SLASH2 version number
	test_cmd = 'slmctl -p version'
	tname = 'slashd'
	bc = boundcmd(cmd=test_cmd,maxtries=1,timeout=5,verbose=options.pdebug)
	(rc,tout,terr)=bc.run()
	versionEval(vhn,tname,test_cmd,rc,tout,terr)

	test_cmd='zpool status -v arc_s2ssd_prod'
	bc = boundcmd(cmd=test_cmd,maxtries=1,timeout=10,verbose=options.pdebug)
	(rc,tout,terr)=bc.run()
	zpoolEval(vhn,'arc_s2ssd_prod',test_cmd,rc,tout,terr)

def highlander():
	# Exit quickly if there is an old script instance running
	myname = os.path.basename(argv[0])
	mypid  = os.getpid()
	lenm = len(myname)
	dprint("DEBUG: using %s as name of this program for comparison" % myname)
	for pid in os.listdir('/proc/') :
		if (not pid.isdigit()) or int(pid) == mypid :
			continue
		ipid = int(pid)
		cmdline = "/proc/%s/cmdline" % pid
		dprint("DEBUG: going to check command line %s for pid %s" % (cmdline,pid))
		try :
			rawcmd = open(cmdline,'r').read()
		except :
			rawcmd = ''
		if rawcmd != '' :
			pidcmd = os.path.basename(rawcmd.split()[0].strip())
			dprint("DEBUG: checking %s -> %s %i against %s %i" % (rawcmd,pidcmd,len(pidcmd),myname,len(myname)))
			if len(pidcmd) -1 == lenm and pidcmd[:lenm] == myname :
				print "ERROR: another instance (PID %s) of %s is still running." % (pid,myname)
#                       try :
#                               os.kill(ipid,signal.SIGKILL)
#                               killedpid, stat = os.waitpid(ipid, os.WNOHANG)
#                               if killedpid == 0:
#                                       print "ACK! PROCESS NOT KILLED?"
#                       except :
#                               print "ERROR: cannot kill old instance"
				exit(1)

def check_list(key,list):
	for item in list:
		if re.match(item,key): return True
	return False

def get_version(args):
	re.search('\$Id.*\$',name).group(0)

################## Main script body ######################################
# Define the rules for interpretting command line arguments
usage = "usage %prog [options]\n\tNOTE:  without any specified options, script will guess\n\twhich tests to run based on hostname"
parser = OptionParser(usage)
parser.add_option("-d", "--dont-send", action="store_true", dest="dontSend", default=False, help="Disable updates to Nagios and syslog servers")
parser.add_option("-f", "--force", action="store_true",dest="force", default=False, help="Bypass checks to prevent multiple instances")
parser.add_option("-q", "--quick", action="store_true", dest="quick", default=False, help="Skip I/O tests that make be slow or block")
parser.add_option("-v", "--verbose", action="store_true", dest="pdebug", default=False, help="Enable verbose debugging information")

(options, args) = parser.parse_args()

if not options.force :
	highlander()

#lookup the local hostname
hn = socket.gethostname().split('.')[0]

# Initialize a global buffer for updates to Nagios
nsca_data = ''

# Some programs (like msctl) require the TERM variable to be set
os.environ["TERM"] = "dumb"

path_munge('/sbin')
path_munge('/bin')
path_munge('/usr/sbin')
path_munge('/usr/bin')
path_munge('/usr/local/sbin')
path_munge('/usr/local/bin')
path_munge('/usr/local/psc/sbin')
path_munge('/usr/local/psc/bin')
path_munge('/local/sbin')
path_munge('/local/bin')
path_munge('/local/psc/sbin')
path_munge('/local/psc/bin')

if hostcfg.has_key(hn):
	h = hostcfg[hn]
	if h.has_key("type") and h["type"] == "mds":
		test_mdserver()
	else:
		test_ioserver()
else:
	# Assume the node is a client
	print "WARNING: no configuration for this host (", hn, "), ", "defaulting to client"
	test_client()

# Calculate an md5sum of this file for reference
payload = ''
try :
	payload = re.search('\$Id.*\$',open(argv[0],'rb').read()).group(0)
finally :
	nsca_queue(hn,'pulse',nagios.ok,payload)

lstate = nagios.unknown
linfo= 'unknown'
lperf= None
try :
	loadavg = os.getloadavg()
	linfo = "load average %3.1f, %3.1f, %3.1f" % loadavg
	lperf = "load1=%f; load5=%f; load15=%f;" % loadavg
	if loadavg[0] < 3 :
		lstate = nagios.ok
		linfo = 'OK - %s' % linfo
	elif loadavg[0]  < 10 :
		linfo = 'WARNING - %s' % linfo
	else :
		linfo = 'CRITICAL - %s' % linfo
finally:
	nsca_queue(hn,'Current Load',lstate,linfo,lperf)

# If the tests queued updates for NSCA, send them before exiting
if nsca_data != '' :
	nsca_send_raw(nsca_data)

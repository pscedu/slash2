#PSC zfs file system errors
# Chris Ganas, Tim Becker

import re, logging, smtplib
import time, gspread, paramiko
import sys, os, signal

from threading import Thread
from argparse import ArgumentParser

import ssh

logger = logging.getLogger("slash2")
hang_file = "zfs-error.hang.pid"

alert_emails = []

class Spread(object):

    toaddr  = 'arch-admin@psc.edu'
    #toaddr  = 'ganaschristopher@gmail.com'

    def __init__(self, user, passw, key):
        #Login to google docs
        logger.debug("Logging into gmail as %s..." % (user))
        g = gspread.login(user, passw)
        self.user = user
        self.passw = passw
        self.key = key

        self.w = g.open_by_key(key).get_worksheet(0)

        #Store entries for lookup
        self.existing_entries = self.retrieve_entries()

        self.send_alert_emails(alert_emails)

    def send_alert_emails(self, emails):
        server = smtplib.SMTP('smtp.gmail.com:587')
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(self.user, self.passw)
        for email in emails:
            headers = ["From: " + self.user,
                    "Subject: %s" % email["subject"],
                    "To: " + self.toaddr,
                    "MIME-Version: 1.0",
                    "Content-Type: text/html"]
            headers = "\r\n".join(headers)
            server.sendmail(self.user, self.toaddr, headers + "\r\n\r\n" + email["body"])
            logger.debug("->%s : %s" % (email["subject"], email["body"]))
        server.quit()

    def retrieve_entries(self):
        return self.w.get_all_values()[1:][:10]

    def find_new(self, zerrors):
        return [z for z in zerrors if not self.exists(z)]

    def exists(self, zerror):
        if hasattr(zerror, 'k_inode'):
            for k in [cell[3] for cell in self.existing_entries]:
                if int(k) == int(zerror.k_inode):
                    return True
        else:
            for b in [cell[4] for cell in self.existing_entries]:
                if b == zerror.back_path:
                    return True
        return False

    def get_bound(self):
        return len(self.existing_entries) + 2

    def add_entries(self, zerrors):
        new_zerrors = self.find_new(zerrors)
        #fork an email?
        if not new_zerrors:
            logger.info("No new errors to report.")
            return

        emails = [{
            subject: "%d New Error(s) Found in ZFS" % len(new_zerrors),
            body: "Errors can be seen at https://docs.google.com/spreadsheet/ccc?key=%s" % (self.key)
        }]

        self.send_alert_emails(emails)

        self.w.update_cells(self.convert_update_cells(new_zerrors))

    def convert_update_cells(self, zerrors):
        bound = self.get_bound()
        new_cells = self.w.range('A{0}:M{1}'.format(bound, bound + len(zerrors)-1))
        i = 0
        for z in zerrors:
            for elem in str(z).split("|"):
                new_cells[i].value = elem
                i += 1
        self.existing_entries = self.retrieve_entries() #update the entries
        return new_cells

class SSH(object):

    def __init__(self, user):
        self.user = user

    #Executed on the senses to find the backpaths of the zerrors which gave only an inode
    def lookup_back_path(self, host, pool, zerrors):

        #Get a list of all the k_inodes for program arguments
        inums = " ".join([str(x.k_inode) for x in zerrors])

        #Search the proper pool for the inode
        cmd = 'sudo /usr/local/psc/sbin/file_lookup.py /arc_sliod/%s/.slmd 1 %s' % (pool, inums)

        out = self.exec_ssh(host, cmd)
        if out:
            out, err = out[:2]
        #Ignore unpredicted output if any
        pat = re.compile("\d+ ")

        if err is not None:
            logger.debug("Error: %s", err)
        logger.debug("Result: %s", out)
        logger.debug("Parsing back_paths...")

        for inode, back_path in [l.split(" ") for l in out if pat.match(l)]:
            logger.debug("Num errors: %d", len(zerrors))

            #Look through remaining zerrors that still need back_path
            for zerror in [z for z in zerrors if z.missing_back_path()]:

                if int(zerror.k_inode) == int(inode):
                    logger.debug("Setting back_path %s", back_path.rstrip())
                    zerror.set_back_path(back_path.rstrip())
        #Any zerrors left in here were not found...
        for zerror in [z for z in zerrors if z.missing_back_path()]:
            zerror.set_message("Failed to lookup path from kernel inode: %s on %s zpool %s" % \
                                (zerror.k_inode, host, pool))

    #Attempts to find the real files on the sense
    def lookup_real_path(self, host, user, zerrors):

        inums = " ".join([str(z.flashback_inode) for z in zerrors])
        #Search the proper pool for the inode
        cmd = 'sudo /usr/local/psc/sbin/file_lookup.py /arc_s2ssd/users/%s 1 %s' % (user, inums)

        out = self.exec_ssh(host, cmd)[0]

        pat = re.compile("\d+ ")

        #Disregard garbage output
        logger.info("Parsing real paths...")
        for inode, path in [l.split(" ") for l in out if pat.match(l)]:

            for zerror in [z for z in zerrrors if z.missing_real_path()]:

                if int(zerror.flashback_inode) == int(inode):

                    logger.info("Setting path to be converted %s", path.rstrip())
                    zerror.set_real_path(path.rstrip())

        #Anything leftover is missing
        for zerror in [z for z in zerrors if z.missing_real_path()]:
            zerror.set_message("Failed to lookup real path from flashback inode: %s on %s " % \
                                (zerror.flashback_inode, host))

    def flashback_lookup(self, host, zerrors):

        #Ignore unpredicted output
        pat = re.compile("\w+ \d+")

        #Stat everything in one large call
        cmd = ""

        for zerror in zerrors:

            assert zerror.ready_for_real_path(),\
                "Was unable to find back_path/flashback -- shouldn't happen"

            #Build stat command to identify file's ownership
            cmd += "sudo stat --printf '%%U %%i %%n\\n' '%s';" %\
                (zerror.flashback_path)

        out = self.exec_ssh(host, cmd)

        if out is None:
            return

        out = out[0]

        #Parse the response from flashback lookup
        for user, inode, flashback_path in [l.split(" ") for l in out if pat.match(l)]:

            for zerror in [z for z in zerrors if not z.has_flashback_stats()]:

                if flashback_path.rstrip() == zerror.flashback_path:
                    logger.debug("Setting host to %s, user to %s, and flashback inode to %s", host, user, inode)
                    zerror.set_flashback_info(host, user, inode)

                    #No user directory?
                    if user == "UNKNOWN": zerror.set_message("No userdir on flashbacks.")

    #Execute arbitrary command and return STDOUT
    def exec_ssh(self, host, cmd, timeout = None):

        #SSH in, assuming passwordless authentication
        try:
            s = ssh.SSH(self.user, host, "")
        except paramiko.SSHException:
            logger.error("Unable to connect to %s@%s!", self.user, host)
            return

        logger.debug("Connected to %s@%s", self.user, host)

        start_time = time.time()

        #Execute the command and get output from STDOUT
        result = s.run(cmd, timeout)

        #Timed out error code
        if result["exit"] != 137:
            logger.debug("Execution took %.2f seconds and returned %s" \
                    % (time.time() - start_time, result["exit"]))
        else:
            logger.critical("command timed out!")

        return [result["out"], result["err"], result["exit"]]

class Zpool(SSH):

    zerrors = []


    def __init__(self, user, sense_size):

        self.sense_size = sense_size

        #Initialize superclass
        super(Zpool, self).__init__(user)

    def find_errors(self):
        logger.info('Checking for errors...')


        buffers = []

        cmd = "sudo zpool status -v"
        try:
            #Write pid just in case it hangs on zpool status
            f = open(hang_file, "w")
            pid = str(os.getpgid(os.getpid()))
            f.write(pid)
            f.close()
            logger.debug("Writing status hang file...")
            #Grab the zpool status from each sense
            for host in ["sense"+str(x) for x in xrange(self.sense_size)]:

                logger.debug('Checking %s for errors...', host)
                #Get status output from STDOUT
                result = super(Zpool, self).exec_ssh(host, cmd, timeout = 3)
                if result is None:
                    continue
                status_hang_file = ".zpool_hang.%s" % host
                alerted = os.path.exists(status_hang_file)

                #Timed out
                if result[2] == 137:
                    if not alerted:
                        logger.critical("zpool status hanged on %s!", host)
                        email = {
                            "subject": "Zpool Status Hang on %s" % host,
                            "body": "Unable to query for zpool status"
                        }
                        alert_emails.append(email)

                        #Indicate we have already emailed about the issue
                        f = open(status_hang_file, "w")
                        f.close()
                    else:
                        logger.warning("zpool is still hanging.")
                elif alerted:
                    #Completed successfully, so we can remove the indication file
                   os.unlink(status_hang_file)
                buffers += result[0]

            logger.info("Parsing status output...")
            for line in buffers:
                #Unpack the rule name, regex, and action function defined
                for rule, (regex, action) in self.ZpoolError.pool_regex.items():
                    match = regex.match(line)
                    if match:
                        if rule == "change_pool": sense, pool = match.groups()
                        else:
                            #If it is not the exception, run the lambda!
                            #Also add it to the internal list of errors

                            assert sense and pool, "Out of order regex -- shouldn't happen"
                            zerr = self.ZpoolError(sense, pool)
                            action(zerr, match.groups())
                            logger.debug("Found zerror: %s", zerr)
                            self.zerrors.append(zerr)
        finally:
            if os.path.isfile(hang_file):
                logger.debug("Deleting status hang file; zpool status isn't hanging.")
                try:
                    os.unlink(hang_file)
                except Exception:
                    logger.warning("Unable to delete hang file?")

    def find_back_paths(self):
        back_paths = {}
        #Sort zerrors into their sense and respective zpool
        #Allows for efficent lookup
        for zerror in [z for z in self.zerrors if z.missing_back_path()]:

            # Create dictionary for sense if it doesn't exist
            if not zerror.sense in back_paths:
                back_paths[zerror.sense] = {}

            # Create list for pool if it doesn't exist
            if not zerror.pool in back_paths[zerror.sense]:
                back_paths[zerror.sense][zerror.pool] = []

            back_paths[zerror.sense][zerror.pool].append(zerror)

        thread_list = []
        # Create thread for each pool
        for sense, error_list in back_paths.items():
            for pool, zerrors in error_list.items():

                host = "sense" + sense

                logger.info("Looking up %d files on %s, pool %s", \
                    len(zerrors), host, pool)

                t = Thread(target=super(Zpool, self).lookup_back_path, \
                    args=(host, pool, zerrors))
                thread_list.append(t)

        #Start all threads. Wait until all of threads return
        [t.start() for t in thread_list]
        [t.join() for t in thread_list]

    def find_owners(self):
        #Convert all of the zerror flashback_paths into real_paths
        for host in ["flashback", "flashback2"]:
            zerrors = [z for z in self.zerrors \
                        if not z.has_flashback_stats() and not z.resolved]
            lookup = super(Zpool, self).flashback_lookup(host, zerrors)

    def find_real_paths(self):
        flashbacks = {}
        for zerror in [z for z in self.zerrors if z.missing_real_path() and not z.resolved]:

            assert zerror.has_flashback_stats(), "Something went wrong finding the owner!"

            if not zerror.flashback_host in flashbacks:
                flashbacks[zerror.flashback_host] = {}

            if not zerror.user in flashbacks[zerror.flashback_host]:
                flashbacks[zerror.flashback_host][zerror.user] = []

            flashbacks[zerror.flashback_host][zerror.user].append(zerror)

        thread_list = []

        # Create thread for each pool
        for host, error_list in flashbacks.items():
            for user, zerrors in error_list.items():

                logger.info("Looking up %d real paths on %s for %s", len(zerrors), host, user)
                #Add lookup to threading pool.
                t = Thread(target=super(Zpool, self).lookup_real_path, \
                        args=(host, user, zerrors))
                thread_list.append(t)

        #Start all threads. Wait until all of threads return
        [t.start() for t in thread_list]
        [t.join() for t in thread_list]

    def check_status(self):

        #Checking for everything in one call
        flashbacks = {}
        for zerror in [z for z in self.zerrors if not z.resolved]:

            if not zerror.flashback_host in flashbacks:
                flashbacks[zerror.flashback_host] = []

            flashbacks[zerror.flashback_host].append(zerror)
        for host, zerrors in flashbacks.items():

            for zerror in zerrors: cmd += "md5sum '%s';" % zerror.real_path

            #md5sum will print to out any 'working' files
            out, err = super(Zpool, self).exec_ssh(host, cmd)

            for md5, real_path in out.split():
                for zerror in zerrors:
                    if zerror.real_path == real_path.rstrip():
                        zerror.set_message("File is reachable by md5sum")

        for zerror in [z for z in self.zerrors if not z.resolved]:
            zerror.set_message("File is unavailable by md5sum")


    def __str__(self):
        return "Zerrors:\n" + "\n".join([str(z) for z in self.zerrors])

    class ZpoolError(object):

        # Regex name : (Regex pattern, if pattern is true -> execute)
        pool_regex = {
            "change_pool" : (
                    #Identify which sense and pool is in question
                    re.compile("\s+pool: sense(\d+)_pool(\d+)"),
                    None
                ),
            "back_path"   : (
                    #Wooh! We don't even have to look it up :)
                    re.compile("\s+(/arc_sliod/.+)"),
                    lambda z, args: z.set_back_path(args[0])
                ),
            "k_inode"     : (
                    re.compile("\s+<0x.+?>:<(0x[a-f0-9]+)>"),
                    #Parse inode and set in base 10
                    lambda z, args: setattr(z, "k_inode", int(args[0], 16))
                )
        }

        def __init__(self, sense, pool):
            self.sense = sense
            self.pool = pool
            self.resolved = False
            self.message = "?"
            self.status = "Unavailable"
            ## k_inode, back_path

        #determine if inode lookup is necessary!
        def missing_back_path(self):
            return not hasattr(self, "back_path")

        def set_back_path(self, back_path):
            self.back_path = back_path
            #/arc_sliod/4/.slmd/3a8cea38614e8122/fidns/2/3/b/2/00000000023b2627_0
            self.slash2_fid = self.back_path.split("/")[-1].split("_")[0].lstrip("0")
            #/arc_s2ssd/.slmd/fidns/fidns/2/3/b/2/00000000023b2627
            #Cleaner way to do this? Not sure how consistent back_path is
            self.flashback_path = "/arc_s2ssd/.slmd/fidns/" + "/".join("_".join(self.back_path.split("_")[:-1]).split("/")[6:])

        def ready_for_real_path(self):
            return hasattr(self, "back_path") \
            and hasattr(self, "flashback_path")

        def has_flashback_stats(self):
            return hasattr(self, "user") and hasattr(self, "flashback_inode") \
                    and hasattr(self, "flashback_host")

        def set_flashback_info(self, host, user, inode):
            self.user = user
            self.flashback_inode = inode
            self.flashback_host = host

        def missing_real_path(self):
            return not hasattr(self, "real_path")

        def set_real_path(self, host, path):
            self.host = host
            self.real_path = "/arc/" + "/".join(path.split("/")[1:])

        def set_message(self, message):
            self.resolved = True
            self.message = message

        def __str__(self):
            return "|".join([str(time.ctime()), str(self.sense), str(self.pool),
                    str(self.k_inode) if hasattr(self, "k_inode") else "-",
                    self.back_path if hasattr(self, "back_path") else "-",
                    "0x"+self.slash2_fid if hasattr(self, "slash2_fid") else "-",
                    str(int(self.slash2_fid, 16)) if hasattr(self, "slash2_fid") else "-",
                    self.flashback_host if hasattr(self, "flashback_host") else "-",
                    self.flashback_path if hasattr(self, "flashback_path") else "-",
                    self.user if hasattr(self, "user") else "-",
                    self.flashback_inode if hasattr(self, "flashback_inode") else "-",
                    self.real_path if hasattr(self, "real_path") else "-",
                    self.message])

def main():
    z = Zpool(os.getenv("USER"), 8)

    logger.info("Finding errors")
    z.find_errors()
    logger.info("Finding back_paths")
    z.find_back_paths()
    logger.info("Finding owners")
    z.find_owners()
    logger.info("Finding real_paths")
    z.find_real_paths()
    logger.info("Checking status and making sure they are really errors")
    z.check_status()

    #manage credentials
    s = Spread('psczfslogger@gmail.com', 'f*2vU%MlpX', '0ArwA4BtPiQgpdFo2cThrUjJHSHMwLUVmMWxLX3FGbFE')
    #s = Spread('psczfslogger@gmail.com', 'f*2vU%MlpX', '0ArwA4BtPiQgpdHctR0lLbWtkOWhxRlItWUpPcllfNXc')
    s.add_entries(z.zerrors)

def kill_halted_proc(pid):
    logger.warning("Attempting to kill pid %s...", pid)
    try:
        os.killpg(pid, signal.SIGKILL)
    except Exception:
        logger.error("Unable to kill process. :( Halting.")
        return False
    logger.info("Successfully killed halted process! Continuing.")
    return True

if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument('-s', '--set-debug', default="INFO",
        help='Sets the debug level specified (WARNING, INFO, DEBUG).')
    parser.add_argument('-l', '--file-log', default=None,
        help='Adds a file log.')
    args = parser.parse_args()

    log_dict = {
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "DEBUG": logging.DEBUG
    }

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    level = logging.INFO

    if args.set_debug in log_dict:
        level = log_dict[args.set_debug]
    else:
        print("Invalid logging level.")
        sys.exit(1)

    logger.setLevel(level)

    ch = logging.StreamHandler()
    ch.name = 'Console Log'
    ch.level = level
    ch.formatter = fmt
    logger.addHandler(ch)

    if args.file_log is not None:
        fh = logging.FileHandler(args.file_log)
        fh.name = "File Log"
        fh.level = level
        fh.formatter = fmt
        logger.addHandler(fh)
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    if os.path.exists(hang_file):
        f = open(hang_file, "r")
        pid = int(f.read(16))
        logger.warning("Looks like the last process hanged...")
        if str(pid) in [f.split()[0] for f in os.popen("ps xa")]:
            if not kill_halted_proc(pid):
                sys.exit(1)
        else:
            logger.info("Hanged process doesn't seem to exist anymore.")
        os.unlink(hang_file)
    main()

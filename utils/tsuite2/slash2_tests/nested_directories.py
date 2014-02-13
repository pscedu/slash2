# %PSCGPL_START_COPYRIGHT%
# -----------------------------------------------------------------------------
# Copyright (c) 2009-2013, Pittsburgh Supercomputing Center (PSC).
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at
# your option) any later version.
#
# This program is distributed WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
# PURPOSE.  See the GNU General Public License contained in the file
# `COPYING-GPL' at the top of this distribution or at
# https://www.gnu.org/licenses/gpl-2.0.html for more details.
#
# Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
# 300 S. Craig Street			e-mail: remarks@psc.edu
# Pittsburgh, PA 15213			web: http://www.psc.edu/
# -----------------------------------------------------------------------------
# %PSC_END_COPYRIGHT%


import os, shutil
from stat import *

test_file = ""

# sets up files and returns dictionary of results
def setup(directory):
    results = {}

    global base_directory
    base_directory = directory + '/a'

    global full_path
    full_path = directory + "/a/a/a/a/a/a/a/a"
    results["pass"] = True

    try:
        os.makedirs(full_path)
    except:
        results["pass"] = False
        results["error"] = 'Could not create the directory'

    return results

# implements test logic and returns dictionary of results
def operate(max_size=3):
    results = {}

    #verify that the directory exists
    try:
        results["pass"] = S_ISDIR(os.stat(full_path).st_mode)
    except:
        results["pass"] = False
        results["error"] = "Could not stat the directory"

    return results

# cleans up files and returns dictionary of results
def cleanup():
    results = {}

    results["pass"] = True

    try:
        shutil.rmtree(base_directory)
    except:
        results["pass"] = False
        results["error"] = 'Could not remove file'

    return results

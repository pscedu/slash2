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


import os 

test_file = ""

# sets up files and returns dictionary of results
def setup(directory):
    results = {}

    global test_file
    test_file = directory + "/f1"

    results["pass"] = True

    try:
        open(test_file, 'a').close()
    except:
        results["pass"] = False
        results["error"] = 'Could not create file'

    return results

# implements test logic and returns dictionary of results
def operate(max_size=3):
    results = {}
    size = 102400

    try:
        f = open(test_file, 'a')
        for i in xrange(size):
            f.seek(1, 1)

        if(f.tell() == size):
            results["pass"] = True
        else:
            results["pass"] = False

        f.close()

    except:
        results["pass"] = False
        results["error"] = "Did not seek to the correct offset"
    return results

# cleans up files and returns dictionary of results
def cleanup():
    results = {}

    results["pass"] = True

    try:
        os.remove(test_file)
    except:
        results["pass"] = False
        results["error"] = 'Could not remove file'

    return results

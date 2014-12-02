import os

test_file = ""

test_env = {
    host:"adamantium",
    env: {
	"MDS":		"mds0@tsuite",
	"PREF_IOS":	"io1@tsuite"
    }
}


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

    data = ''.join([chr(a + 97) for a in xrange(26)]) # a-z

    try:
	# write data
	f = open(test_file, 'w')
	f.write(data)
	f.close()
    except:
	results["pass"] = False
	results["error"] = 'Could not write to file'

    try:
	#read data
	f = open(test_file, 'r')
	read_data = f.read(len(data))

	#verify that data is there and at end of file
	results["pass"] = read_data == data and len(f.read(1)) == 0
	f.close()
    except:
	results["pass"] = False
	results["error"] = 'Could not read from file'

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



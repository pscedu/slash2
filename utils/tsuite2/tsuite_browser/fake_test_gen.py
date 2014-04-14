import json, random

tsets = 100
tests = [
    ["basic_read", 10],
    ["basic_write", 10],
    ["complex_read", 20],
    ["multi_write", 20],
    ["huge_seek", 15],
    ["dev_zero", 5],
    ["scatter", 20],
    ["huge_file", 30],
    ["batch_insert", 40],
    ["test_test", 20]
]

tests = sorted(tests, key = lambda k: k[0])

db = []

for i in range(tsets):
    tset =  {}
    #random.shuffle(tests)

    failed = 0

    if random.random() > 0.9:
        failed = random.randrange(0, len(tests))

    tset["tset_name"] = "#"+str(i+1)
    tset["tsid"] = i+1
    tset["failed_tests"] = failed
    tset["total_tests"] = len(tests)
    tset["tests"] = []
    tset["total_time"] = 0.0

    for j in range(len(tests)):
        test_name = tests[j][0]
        time = tests[j][1]
        if i > 0:
            if random.random() > 0.80:
                l, h = 0.8, 1.2
            else:
                l, h = 0.95, 1.05
            time = db[i-1]["tests"][j]["elapsed"] * random.uniform(l, h)
        test = {
            "test_name": test_name,
            "desc": "generated test.",
            "pass": j >= failed,
            "msg": "" if j >= failed else "problem!",
            "elapsed": time
        }
        tset["total_time"] += round(time, 2)
        tset["tests"].append(test)
    tset["tests"] = sorted(tset["tests"], key = lambda k: k["test_name"])
    db.append(tset)
print json.dumps(db, indent=True)

import json, random

tsets = 200
tests = [
    ["basic_read", 10],
    ["basic_write", 10],
    ["complex_read", 20],
    ["multi_write", 20],
    #["huge_seek", 15],
    #["dev_zero", 5],
    #["scatter", 20],
    #["huge_file", 30],
    #["batch_insert", 40],
    #["test_test", 20]
]

setup = {
        "resources": {
  "mds": [
   {
    "zpool_path": "/local/sl2_pool",
    "name": "mds1",
    "fsuuid": "0x1337beef",
    "jrnldev": "/dev/sdh1",
    "zpool_args": "/dev/sdm1",
    "site_id": "10",
    "site": "MYSITE",
    "zpool_cache": "/tmp/tsuite/sltest.1401047/sl2_pool.zcf",
    "host": "localhost",
    "zpool_name": "sl2_pool",
    "type": "mds",
    "id": "1"
   }
  ],
  "client": [],
  "ion": [
   {
    "fsroot": "/local/cg.s2",
    "name": "ion1",
    "fsuuid": "0x1337beef",
    "site_id": "10",
    "site": "MYSITE",
    "host": "localhost",
    "prefmds": "mds1@MYSITE",
    "type": "standalone_fs",
    "id": "2"
   }
  ]
 }
}
clients = ["stark", "lannister", "grayjoy", "baratheon", "dorn", "bravos"]
#clients = ["stark", "lannister"]

tests = sorted(tests, key = lambda k: k[0])

db = []

for i in range(tsets):
    tset =  {}
    #random.shuffle(tests)

    tset["tset_name"] = "#"+str(i+1)
    tset["tsid"] = i+1
    tset["failed_tests"] = 0
    tset["total_tests"] = len(tests)
    tset["tests"] = {}
    tset["total_time"] = 0.0
    tset["resources"] = setup["resources"]
    tset["resources"]["client"] = []


    for c in clients:
        tset["resources"]["client"].append(
        {
            "host": c,
            "type": "client",
            "name": c,
            "site": 100
        })

    for j in range(len(tests)):

        test = {}
        test_name = tests[j][0]

        test[test_name] = []

        for client in clients:
            time = tests[j][1] * random.uniform(.80, 1.20)

            """
            if i > 0:
                if random.random() > 0.70:
                    l, h = 0.8, 1.2
                else:
                    l, h = 0.95, 1.05
                try:
                    past_clients = db[i-1]["tests"][j-1][test_name]
                except Exception:
                    continue
                for p in past_clients:
                    if p["client"] == client:
                        time = p["result"]["elapsed"] * random.uniform(l, h)
            """

            client_test = {
                "client": client,
                "result": {
                    "pass": True,
                    "retcode": 0,
                    "name": test_name,
                    "elapsed": time
                }
            }

            if  random.random() > 0.9:
                if 0.5 <= random.random():
                    tset["failed_tests"] += 1
                    client_test["result"]["pass"] = False
                    client_test["result"]["retcode"] = random.randint(1, 100)
                else:
                    continue

            test[test_name].append(client_test)
            tset["total_time"] += round(time, 2)
        tset["tests"][test_name] = test[test_name]
    db.append(tset)
print json.dumps(db, indent=True)

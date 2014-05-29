import json, random

tsets = 20
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

clients = ["stark", "lannister", "grayjoy", "baratheon"]
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
    tset["tests"] = []
    tset["total_time"] = 0.0

    test = {}
    for j in range(len(tests)):
        test_name = tests[j][0]
        test[test_name] = []

        for client in clients:
            time = tests[j][1]

            if i > 0:
                if random.random() > 0.80:
                    l, h = 0.8, 1.2
                else:
                    l, h = 0.95, 1.05
                past_clients = db[i-1]["tests"][j][test_name]
                for p in past_clients:
                    if p["client"] == client:
                        time =  p["result"]["elapsed"] * random.uniform(l, h)

            client_test = {
                "client": client,
                "result": {
                    "pass": True,
                    "retcode": 0,
                    "name": test_name,
                    "elapsed": time
                }
            }

            if  random.random() > 0.8:
                if 0.5 <= random.random():
                    tset["failed_tests"] += 1
                    client_test["result"]["pass"] = False
                    client_test["result"]["retcode"] = random.randint(1, 100)
                else:
                    continue

            test[test_name].append(client_test)
            tset["total_time"] += round(time, 2)
        tset["tests"].append(test)
    db.append(tset)
print json.dumps(db, indent=True)

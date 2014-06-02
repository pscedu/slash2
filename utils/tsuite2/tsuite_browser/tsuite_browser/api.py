from flask.ext.pymongo import PyMongo

class API(object):

    def __init__(self, app):
        self.app = app
        self.mongo = PyMongo(app)

    def _error(self, errid, msg):
        """Returns a generic error response."""

        return {
            "err": errid,
            "err_msg": msg
        }

    def get_tset(self, tsid):
        """Get a specific slash2 test set.

        Args:
            tsid: tsid of the set.
        Returns:
            tset if it is found, else None"""

        return self.mongo.db.tsets.find_one({"tsid": tsid})

    def get_tsets(self, limit=None, sort=-1):
        """List all tsets."""
        result = self.mongo.db.tsets.find().sort([("_id", sort),])
        if limit:
           result = result.limit(limit)
        return list(result)

    def get_latest_tset(self):
        """Get last slash2 test set.

        Returns:
            Returns the latest set if it exists, None elsewise."""

        tset = self.mongo.db.tsets.find().sort([("_id", -1),])
        return None if tset.count() == 0 else tset[0]

    def get_neighboring_tests(self, tsid, test_name, positions):
        """Get neighboring test results n positions away from tsid.

        Args:
            tsid: test set id to serve as center.
            test_name: test to be found.
            positions: how many spaces away relative to tsid.

        Returns:
            list of relevant tsets."""

        tsets = self.get_tsets(sort=1)
        tsid -= 1

        lower, higher, adj_tests = {}, {}, {}

        for test in tsets[tsid]["tests"]:
            test_name = test["test_name"]

            lower[test_name] = []
            higher[test_name] = []
            adj_tests[test_name] = []

        for test_name in lower.keys():
            i = tsid - 1
            while i >= 0 and i < tsid and len(lower[test_name]) < positions:
                for test in tsets[i]["tests"]:
                    if test["test_name"] == test_name and test["pass"]:
                        test["tsid"] = i+1
                        assert(i+1 == test["tsid"])
                        lower[test_name] = [test] + lower[test_name]
                i -= 1
            i = tsid
            while i < len(tsets) and len(higher[test_name]) < positions:
                for test in tsets[i]["tests"]:
                    if test["test_name"] == test_name and test["pass"]:
                        test["tsid"] = i+1
                        higher[test_name].append(test)
                i += 1

        for test_name in higher.keys():
          llen, hlen = len(lower[test_name]), len(higher[test_name])

          m = min(llen, hlen)

          lindex = m
          hindex = m

          if llen >= hindex:
            lindex = max(positions - 2*m, llen)
          else:
            hindex = max(positions - 2*m, hlen)

          print lindex, hindex

          for l in range(lindex):
              adj_tests[test_name].append(lower[test_name][l])
          for h in range(hindex):
              adj_tests[test_name].append(higher[test_name][h])

        return adj_tests

    def get_tset_averages(self, tset):
        """Get averages over all clients of the tests in a single tset.

        Args:
            tset: tset to get the average test data from."""

        test_averages = {}

        print tset

        for test_name, clients in tset["tests"].items():
            print test_name, clients

            valid_clients = 0
            test_averages[test_name] = {
                "average": 0.0,
                "passed": 0,
                "failed": 0
            }


            for client in clients:
                if client["result"]["pass"]:
                    test_averages[test_name]["average"] += client["result"]["elapsed"]
                    test_averages[test_name]["passed"] += 1
                else:
                    test_averages[test_name]["failed"] += 1
            if test_averages[test_name]["average"] != 0.0:
                test_averages[test_name]["average"] /= test_averages[test_name]["passed"]
        return test_averages

    def get_tset_display(self, tsid):
        """Get tset ready for display with simple statistics.

        Args:
            tsid: tset id for display."""


        tset = self.get_tset(tsid)
        tset_averages = self.get_tset_averages(tset)
        recent_tsets = list(self.mongo.db.tsets.find(
            {
                "tsid": {"$lt": tsid},
            }
        ).sort([("_id", -1),]))

        #Needs optimized
        for test in tset_averages:
            for old_tset in recent_tsets:
                recent_averages = self.get_tset_averages(old_tset)
                if test in recent_averages:
                    tset_averages[test]["change_delta"] = tset_averages[test]["average"] - recent_averages[test]["average"]
                    tset_averages[test]["change_percent"] = round(tset_averages[test]["change_delta"] / max(1, recent_averages[test]["average"]), 3) * 100
                    tset_averages[test]["change_tsid"] = old_tset["tsid"]
                    break

        return tset_averages


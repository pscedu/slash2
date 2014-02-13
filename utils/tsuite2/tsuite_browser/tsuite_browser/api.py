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

    def get_tsets(self, lim):
        """List all tsets."""

        return list(self.mongo.db.tsets.find().sort([("_id", -1),]).limit(lim))

    def get_latest_tset(self):
        """Get last slash2 test set.

        Returns:
            Returns the latest set if it exists, None elsewise."""

        tset = self.mongo.db.tsets.find().sort([("_id", -1),])
        return None if tset.count() == 0 else tset[0]

    def get_tset_display(self, tsid):
        """Get tset ready for display with simple statistics.

        Args:
            tsid: tset id for display."""

        tset = self.get_tset(tsid)
        for test in tset["tests"]:
            most_recent_tests = list(self.mongo.db.tsets.find(
                {
                    "tsid": {"$lt": tsid},
                    "tests": {
                        "$elemMatch": {"test_name": test["test_name"], "pass": True}
                    }
                },
                {
                    "tsid": "",
                    "tests":{
                        "$elemMatch": {"test_name": test["test_name"], "pass": True}
                    }
                }
            ).sort([("_id", -1),]).limit(1))
            if len(most_recent_tests) == 0:
                test["change_percent"] = None
            else:
                print most_recent_tests
                recent_test = most_recent_tests[0]["tests"][0]
                test["change_delta"] = test["elapsed"] - recent_test["elapsed"]
                test["change_percent"] = round(test["change_delta"] / recent_test["elapsed"], 3) * 100
                test["change_tsid"] = most_recent_tests[0]["tsid"]
        return tset


import pymongo, sys

class MongoHelper(object):

  def __init__(self, conf):
    self.host = conf["mongo"]["host"]
    mongo = pymongo.Connection(self.host)
    self.col = mongo["tsuite_browser"]["tsets"]

  def get_tsets(self, limit=None, sort=-1):
      """List all tsets."""
      result = self.col.find().sort([("_id", sort),])
      if limit:
          result = result.limit(limit)
      return list(result)

  def get_latest_tset(self):
      """Get last slash2 test set.

      Returns:
          Returns the latest set if it exists, None elsewise."""

      tset = self.col.find().sort([("_id", -1),]).limit(1)
      return None if tset.count() == 0 else tset[0]

  def get_tset(self, tsid):
      """Get a specific slash2 test set.

      Args:
          tsid: tsid of the set.
      Returns:
          tset if it is found, else None"""

      return self.col.find_one({"tsid": tsid})

if __name__=="__main__":
  a = MongoHelper({"mongo":{"host":"localhost"}})
  print a.get_latest_tset()

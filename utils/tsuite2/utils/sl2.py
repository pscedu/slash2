import logging

log = logging.getLogger("sl2.res")

#TODO: Already defined in tsuite
def check_subset(necessary, check):
  """Determines missing elements from necessary list.

  Args:
    necessary: list of mandatory objects.
    check: list to be checked.
  Returns:
    List of elements missing from necessary."""

  if not all(n in check for n in necessary):
    #Remove sections that are correctly there
    present = [s for s in check if s in necessary]
    map(necessary.remove, present)
    return necessary
  return []

class SL2Res(dict):
  """Represents slash2 objects. MDS, ION, etc."""

  #Necessary fields for resources
  necessary = {
    "mds"    : [
      "zpool_args",
      "fsuuid",
      "host",
      "site_id",
      "zpool_path"
    ],
    "all_but_mds": [
      "prefmds",
      "host",
      "site_id"
    ]
  }

  def __init__(self, name, site):
    """Initialize sl2 object.

    Args:
      name: name of resource.
      site: siteid of resource."""

    dict.__init__(self)
    dict.__setitem__(self, "name", name)
    dict.__setitem__(self, "site", site)

  def finalize(self, sl2objects):
    """Check for missing fields and add to sl2objects.

    Args:
      sl2objects: dict of sl2objects from tsuite."""

    sl2res_type = None
    sl2res_name = dict.__getitem__(self, "name")

    #Check for type; type is required.
    try:
      sl2res_type = dict.__getitem__(self, "type")
    except KeyError:
      log.fatal("Resource named: {0} is missing a type!"\
          .format(sl2res_name))
      return False

    if sl2res_type in  ["standalone_fs", "archival_fs"]:
      sl2res_type = "ion"

    #Lookup necessary fields for object type
    necessary_fields = self.necessary[sl2res_type]\
        if sl2res_type in self.necessary else []

    if sl2res_type != "mds":
      #TODO: evaluate the necessity of this...
      #necessary_fields += self.necessary["all_but_mds"]
      pass

    missing = check_subset(necessary_fields, dict.keys(self))
    if len(missing) != 0:
      log.fatal("Missing fields from {0} res named {1}!"\
          .format(sl2res_type, sl2res_name))
      log.fatal("Missing: {0}".format(", ".join(missing)))
      return False

    #Create/append the resource type to sl2objects
    if sl2res_type not in sl2objects:
      sl2objects[sl2res_type] = []

    sl2objects[sl2res_type].append(self)

    return True


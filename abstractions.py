from datetime import datetime, date, time
import subprocess
import re

class hdfsListParser:
  def __init__(self):
    self.hdfsListRegex = """^(-|d)((-|[a-z]){3}){3}\s{1,5}.+([0-9]{4}-[0-9]{2}-[0-9]{2}).+\:.+\s{1,10}(\/.*)$"""
    # To validate this, throw it in regex101.com with one of the input.txt mock input lines.
    # It's a good way to visualize what matches what and what the groups are/mean

  def __returnGroup__(self,groupNumber, line):
    regex = re.match(self.hdfsListRegex, line)
    if (regex):
      return regex.group(groupNumber)
    else:
      return None

  def hdfsDate(self, line):
    return self.__returnGroup__(4, line)
  def getIsFile(self, line):
    return (True if self.__returnGroup__(1, line) == '-' else False)
  def getIsDir(self, line):
    return (True if self.__returnGroup__(1, line) == 'd' else False)

class hdfs:
  """ HDFS Interface. Does the 2 things we need right now: List Rec.; Delete """
  # We could likely implement a method for `path` input validation
  def list(self, path):
    try:
      return subprocess.check_output(["hadoop", "fs", "-ls", "-R", path])
    except:
      print("ERROR: Hadoop list command failed, please check the host")
      exit(7)
  def delete(self, hdfsObj, dry_run=False):
    # Getting a bit more details from hdfsObj :
    dtAge = datetime.strptime(hdfsObj.date, "%Y-%m-%d")
    tDiff = (datetime.now() - dtAge)
    # Try,catch with logging if deletion fails.
    try:
      # WARNING: NEVER turn on the '-R' switch on the hadoop fs -rm cmd below.
      # Even if you think you know what you're doing, think/verify 5 times.
      # If you decide to go ahead with that, check the trash bin settings of your HDFS.
      if not dry_run:
        print("File name %s -> TTL is %s and Date is %s , Age is %i days" % (hdfsObj.path, hdfsObj.ttl, hdfsObj.date, tDiff.days))
        print(subprocess.check_output(["hadoop", "fs", "-rm", hdfsObj.path]))
      else:
        print("File name %s -> TTL is %s and Date is %s , Age is %i days" % (hdfsObj.path, hdfsObj.ttl, hdfsObj.date, tDiff.days))
        print("WOULD HAVE removed : %s" % hdfsObj.path)
    except Exception as e:
      print(e)
      print("ERROR: Could not remove the file, please check")
      # Make it fatal on the first iterations, we can lift if
      # it proves to be legit by observation
      exit(13)

class hdfsObject(hdfsListParser):
  def __init__(self, line):
    self.raw = line
    self.hdfsListRegex = """^(-|d)((-|[a-z]){3}){3}\s{1,5}.+([0-9]{4}-[0-9]{2}-[0-9]{2}).+\:.+\s{1,10}(\/.*\/ttl=([0-9]{1,4}).*)$"""
    self.ttl = int(self.__returnGroup__(6, line))
    self.date = self.__returnGroup__(4, line)
    self.isFile = self.getIsFile(line)
    self.isDir = self.getIsDir(line)
    self.path = self.__returnGroup__(5, line)

  def hdfsDate(self, line):
    return self.__returnGroup__(4, line)

  # Maybe naming of this method can improve. Open to suggestions:
  def olderThanTtl(self):
    # TODO: Write olderThanX() so this is a specific case of it.
    dtAge = datetime.strptime(self.date, "%Y-%m-%d")
    tDiff = (datetime.now() - dtAge)
    if ( int(tDiff.days) > self.ttl ):
      return True
    else:
      return False

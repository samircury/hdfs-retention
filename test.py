#!/usr/bin/python

from abstractions import hdfs
from abstractions import hdfsListParser
from abstractions import hdfsObject
import subprocess
import unittest
import sys
import re

# Get input from listing
  # Overriding list method for testing with local mocked file:
class hdfsMock(hdfs):
  def list(self,path):
    return subprocess.check_output(["cat", path])

class parseTest(unittest.TestCase):
  def setUp(self):
    self.dirString = 'drwxr-xr-x   - gandalf gandalf          1 2019-05-29 01:54 /user/gandalf/myproduct/ttl=90/d8e5ec08-9843-4099-8f77-5b6234f1ea77/in-app'
    self.nonDirString = '-rwxr-xr-x   3 gandalf gandalf    2527437 2019-05-29 02:01 /user/gandalf/myproduct/ttl=90/d8e5ec08-9843-4099-8f77-5b6234f1ea77/in-app/anx/data/part-00038-02851da4-b3d1-4bb7-a064-a2309385284e-c000.csv.gz'

  def testParsingMinimal(self):
    """We get a sane date out of parsing sane input"""
    self.assertEquals(hdfsListParser().hdfsDate(self.dirString), '2019-05-29')

  def testDirBool(self):
    """Parsing a dir line returns getIsDir() True for a dir and False for a file"""
    self.assertTrue(hdfsListParser().getIsDir(self.dirString))
    self.assertFalse(hdfsListParser().getIsDir(self.nonDirString))
  def testFileBool(self):
    """Parsing a file line returns getIsFile() False for a dir and True for a file"""
    self.assertTrue(hdfsListParser().getIsFile(self.nonDirString))
    self.assertFalse(hdfsListParser().getIsFile(self.dirString))

  def testParsingDateFromFile(self):
    """Do we fetch sane dates from a fs -ls mocked file?"""
    for line in hdfsMock().list("input.txt").splitlines():
      fileDate = hdfsListParser().hdfsDate(line)
      if (hdfsListParser().hdfsDate(line)):
        self.assertTrue(re.match('[0-9]{4}(-[0-9]{2}){2}',fileDate))


class ttlTests(unittest.TestCase):
  def setUp(self):
    self.ttlNewFile = '-rwxr-xr-x   3 gandalf gandalf    2527437 2019-05-29 02:01 /user/gandalf/myproduct/ttl=90/d8e5ec08-9843-4099-8f77-5b6234f1ea77/in-app/anx/data/part-00038-02851da4-b3d1-4bb7-a064-a2309385284e-c000.csv.gz'
    self.incrementalOldFile = '-rwxr-xr-x   3 gandalf gandalf    1341486 2019-01-21 01:45 /user/gandalf/myproduct/incremental/ttl=30/041c44ba-82c0-47d1-bdf9-4cef827cb1fd/in-app/dfp/data/aaid/part-00153-7e6a1b08-607b-4616-b99c-d70f9ef59685-c000.csv.gz'
    self.incrementalNewFile = '-rwxr-xr-x   3 gandalf gandalf    1341486 2019-07-21 01:45 /user/gandalf/myproduct/incremental/ttl=30/041c44ba-82c0-47d1-bdf9-4cef827cb1fd/in-app/dfp/data/aaid/part-00153-7e6a1b08-607b-4616-b99c-d70f9ef59685-c000.csv.gz'
    self.incrementalOldFileShortTtl = '-rwxr-xr-x   3 gandalf gandalf    1341486 2019-07-21 01:45 /user/gandalf/myproduct/incremental/ttl=10/041c44ba-82c0-47d1-bdf9-4cef827cb1fd/in-app/dfp/data/aaid/part-00153-7e6a1b08-607b-4616-b99c-d70f9ef59685-c000.csv.gz'

  def testGetTtlFile(self):
    hdfsObj = hdfsObject(self.ttlNewFile)
    self.assertEquals(hdfsObj.ttl, 90)
    self.assertNotEqual(hdfsObj.ttl, 80)

  def testGetTtlIncrementalFile(self):
    hdfsObj = hdfsObject(self.incrementalOldFile)
    self.assertEquals(hdfsObj.ttl, 30)
    self.assertNotEqual(hdfsObj.ttl, 20)

class hdfsObjTests(unittest.TestCase):
  def setUp(self):
    self.ttlOldDir = 'drwxr-xr-x   - gandalf gandalf          1 2018-09-29 01:54 /user/gandalf/myproduct/ttl=90/d8e5ec08-9843-4099-8f77-5b6234f1ea77/in-app'
    self.ttlNewFile = '-rwxr-xr-x   3 gandalf gandalf    2527437 2019-05-29 02:01 /user/gandalf/myproduct/ttl=90/d8e5ec08-9843-4099-8f77-5b6234f1ea77/in-app/anx/data/part-00038-02851da4-b3d1-4bb7-a064-a2309385284e-c000.csv.gz'

  def testInstantiateHdfsObject(self):
    """Do we get an object with all attributes?"""
    hdfsObj = hdfsObject(self.ttlNewFile)
    self.assertEquals(hdfsObj.ttl, 90)
    self.assertTrue(hdfsObj.isFile)
    self.assertFalse(hdfsObj.isDir)

if __name__ == '__main__':
  suite = unittest.TestLoader().loadTestsFromModule( sys.modules[__name__] )
  unittest.TextTestRunner(verbosity=3).run( suite )

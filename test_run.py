#!/usr/bin/python

from abstractions import hdfs
from abstractions import hdfsListParser
from abstractions import hdfsObject
from datetime import datetime, date, time
import subprocess
import argparse
import unittest
import sys
import re

class hdfsMock(hdfs):
    def list(self, path):
        return subprocess.check_output(["cat", path])
    def delete(self, hdfsObj, dry_run=False):
        dtAge = datetime.strptime(hdfsObj.date, "%Y-%m-%d")
        tDiff = (datetime.now() - dtAge)
        print("File name %s -> TTL is %s and Date is %s , Age is %i days" % (hdfsObj.path, hdfsObj.ttl, hdfsObj.date, tDiff.days))
        print("WOULD HAVE removed : %s" % hdfsObj.path)

class actualRunSimulation(unittest.TestCase):
    def testRealRunFromFile(self):
        """This would emulate main() of the real deletion script"""
        parser = argparse.ArgumentParser()
        parser.add_argument("path")
        parser.add_argument("--dry-run", type=bool, default=False)
        args = parser.parse_args()
        for line in hdfsMock().list("input.txt").splitlines():
            fileDate = hdfsListParser().hdfsDate(line)
            isFile = hdfsListParser().getIsFile(line)
            # Maybe add a check "if file is old enough" below
            if (isFile and fileDate ): # This already does input validation
                # If we got this far, it's worth storing state in memory
                hdfsObj = hdfsObject(line)
                if (hdfsObj and hdfsObj.olderThanTtl()):
                    hdfsMock().delete(hdfsObj, dry_run=args.dry_run)

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromModule( sys.modules[__name__] )
    unittest.TextTestRunner(verbosity=3).run( suite )

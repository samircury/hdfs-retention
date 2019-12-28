#!/usr/bin/env python
from abstractions import hdfs
from abstractions import hdfsListParser
from abstractions import hdfsObject
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--dry-run", type=bool, default=False)
    args = parser.parse_args()

    print("Going to cleanup %s according to its filesystem TTL setting" % args.path)

    for line in hdfs().list(args.path).splitlines():
        fileDate = hdfsListParser().hdfsDate(line)
        isFile = hdfsListParser().getIsFile(line)

        if (isFile and fileDate ): # This already does input validation
            # If we got this far, it's worth storing state in memory
            hdfsObj = hdfsObject(line)

            if (hdfsObj and hdfsObj.olderThanTtl()):
                hdfs().delete(hdfsObj, dry_run=args.dry_run)
main()
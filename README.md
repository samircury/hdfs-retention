# hdfs-retention
Small Set of scripts for SAFE lifecycle of files in HDFS.

I tried to keep it clean and nice on abstractions, but didn't mind packaging it in a very formal way. It's not intended to be on pip or anything.

This was intended to be a Jenkins job, so we're counting on the whole repo to be 
checked out.

# How to use
```
./cleanup-job.py /my/filesystem/path/
```
Or, you may want to do a dry-run first :
```
./cleanup-job.py --dry-run /my/filesystem/path/
```
In this case you'll only get print statements of `WOULD HAVE removed : <file>` in STDOUT, it's great for first-time visual inspections or smoke tests of changes with
the same codepaths used in the cleanup
# Developing
While changing the code, these 2 tools will help you *test* your code and *verify* that it
runs with real input(.txt).

## Testing :

```
$ ./test.py
testInstantiateHdfsObject (__main__.hdfsObjTests)
Do we get an object with all attributes? ... ok
testDirBool (__main__.parseTest)
Parsing a dir line returns getIsDir() True for a dir and False for a file ... ok
testFileBool (__main__.parseTest)
Parsing a file line returns getIsFile() False for a dir and True for a file ... ok
testParsingDateFromFile (__main__.parseTest)
Do we fetch sane dates from a fs -ls mocked file? ... ok
testParsingMinimal (__main__.parseTest)
We get a sane date out of parsing sane input ... ok
testGetTtlFile (__main__.ttlTests) ... ok
testGetTtlIncrementalFile (__main__.ttlTests) ... ok

----------------------------------------------------------------------
Ran 7 tests in 0.011s

OK
```

The above is how to run and the expected output from the unit tests.

## Testing on run with synthetic input
In this repo there is a script ~1:1 to the real cleanup one. However it has some
overrides in the HDFS handling class to *never attempt any deletion*, just to do print statements.

It also uses a pre-baked fake input file (input.txt). Here's how to use it :

```
 ./test_run.py  /user/gandalf/myproduct/incremental
$ ./test_run.py  /user/gandalf/myproduct/incremental
testRealRunFromFile (__main__.actualRunSimulation)
This would emulate main() of the real deletion script ... File name /user/gandalf/myproduct/ttl=90/d8e5ec08-9843-4099-8f77-5b6234f1ea77/in-app/anx/data/_SUCCESS -> TTL is 90 and Date is 2019-05-29 , Age is 213 days
WOULD HAVE removed : /user/gandalf/myproduct/ttl=90/d8e5ec08-9843-4099-8f77-5b6234f1ea77/in-app/anx/data/_SUCCESS
File name /user/gandalf/myproduct/ttl=90/d8e5ec08-9843-4099-8f77-5b6234f1ea77/in-app/anx/data/part-00000-02851da4-b3d1-4bb7-a064-a2309385284e-c000.csv.gz -> TTL is 90 and Date is 2019-05-29 , Age is 213 days
.
.
.
```
You can also see the expected output above

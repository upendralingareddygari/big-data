Logging initialized using configuration in jar:file:/local/apache-hive-0.13.1/lib/hive-common-0.13.1.jar!/hive-log4j.properties
OK
Time taken: 0.604 seconds
OK
Time taken: 0.498 seconds
OK
Time taken: 0.011 seconds
OK
Time taken: 0.049 seconds
OK
Time taken: 0.011 seconds
OK
Time taken: 0.046 seconds
Total jobs = 7
Launching Job 1 out of 7
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1415367758724_3472, Tracking URL = http://cshadoop1.utdallas.edu:8088/proxy/application_1415367758724_3472/
Kill Command = /usr/local/hadoop-2.4.1/bin/hadoop job  -kill job_1415367758724_3472
Hadoop job information for Stage-3: number of mappers: 2; number of reducers: 0
2014-11-11 00:25:45,482 Stage-3 map = 0%,  reduce = 0%
2014-11-11 00:25:50,674 Stage-3 map = 50%,  reduce = 0%, Cumulative CPU 1.87 sec
2014-11-11 00:25:52,741 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 4.12 sec
MapReduce Total cumulative CPU time: 4 seconds 120 msec
Ended Job = job_1415367758724_3472
Stage-6 is filtered out by condition resolver.
Stage-5 is selected by condition resolver.
Stage-7 is filtered out by condition resolver.
Stage-12 is filtered out by condition resolver.
Stage-11 is selected by condition resolver.
Stage-13 is filtered out by condition resolver.
Stage-18 is filtered out by condition resolver.
Stage-17 is selected by condition resolver.
Stage-19 is filtered out by condition resolver.
Launching Job 5 out of 7
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1415367758724_3473, Tracking URL = http://cshadoop1.utdallas.edu:8088/proxy/application_1415367758724_3473/
Kill Command = /usr/local/hadoop-2.4.1/bin/hadoop job  -kill job_1415367758724_3473
Hadoop job information for Stage-5: number of mappers: 1; number of reducers: 0
2014-11-11 00:25:59,593 Stage-5 map = 0%,  reduce = 0%
2014-11-11 00:26:04,761 Stage-5 map = 100%,  reduce = 0%, Cumulative CPU 1.16 sec
MapReduce Total cumulative CPU time: 1 seconds 160 msec
Ended Job = job_1415367758724_3473
Launching Job 6 out of 7
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1415367758724_3474, Tracking URL = http://cshadoop1.utdallas.edu:8088/proxy/application_1415367758724_3474/
Kill Command = /usr/local/hadoop-2.4.1/bin/hadoop job  -kill job_1415367758724_3474
Hadoop job information for Stage-11: number of mappers: 1; number of reducers: 0
2014-11-11 00:26:10,507 Stage-11 map = 0%,  reduce = 0%
2014-11-11 00:26:15,650 Stage-11 map = 100%,  reduce = 0%, Cumulative CPU 1.04 sec
MapReduce Total cumulative CPU time: 1 seconds 40 msec
Ended Job = job_1415367758724_3474
Launching Job 7 out of 7
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1415367758724_3475, Tracking URL = http://cshadoop1.utdallas.edu:8088/proxy/application_1415367758724_3475/
Kill Command = /usr/local/hadoop-2.4.1/bin/hadoop job  -kill job_1415367758724_3475
Hadoop job information for Stage-17: number of mappers: 1; number of reducers: 0
2014-11-11 00:26:22,414 Stage-17 map = 0%,  reduce = 0%
2014-11-11 00:26:27,577 Stage-17 map = 100%,  reduce = 0%, Cumulative CPU 1.33 sec
MapReduce Total cumulative CPU time: 1 seconds 330 msec
Ended Job = job_1415367758724_3475
Loading data to table default.movies_2009
rmr: DEPRECATED: Please use 'rm -r' instead.
Deleted hdfs://cshadoop1/user/hive/warehouse/movies_2009
Loading data to table default.movies_2010
rmr: DEPRECATED: Please use 'rm -r' instead.
Deleted hdfs://cshadoop1/user/hive/warehouse/movies_2010
Loading data to table default.movies_2011
rmr: DEPRECATED: Please use 'rm -r' instead.
Deleted hdfs://cshadoop1/user/hive/warehouse/movies_2011
Table default.movies_2009 stats: [numFiles=1, numRows=0, totalSize=53595, rawDataSize=0]
Table default.movies_2010 stats: [numFiles=1, numRows=0, totalSize=54483, rawDataSize=0]
Table default.movies_2011 stats: [numFiles=1, numRows=0, totalSize=55436, rawDataSize=0]
MapReduce Jobs Launched: 
Job 0: Map: 2   Cumulative CPU: 4.12 sec   HDFS Read: 167957 HDFS Write: 163889 SUCCESS
Job 1: Map: 1   Cumulative CPU: 1.16 sec   HDFS Read: 53978 HDFS Write: 53595 SUCCESS
Job 2: Map: 1   Cumulative CPU: 1.04 sec   HDFS Read: 54866 HDFS Write: 54483 SUCCESS
Job 3: Map: 1   Cumulative CPU: 1.33 sec   HDFS Read: 55819 HDFS Write: 55436 SUCCESS
Total MapReduce CPU Time Spent: 7 seconds 650 msec
OK
Time taken: 51.899 seconds

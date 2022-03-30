YAML parser will receive a test yaml file as an input. 

A test yaml file should have the following parameters:

* test_id
* object_size
* part_size
* range_read
* part_range
* min_runtime
* sessions_per_node
* sessions
* tool

---

TEST_ID can be any unique string which can be treated as filtering through execution logs.  

object_size & part_size can be given as range for test workload, so those can have the following 
keys 
in it.
* start
* end

Sessions_per_node and sessions is a multiplying factor which is used to calculate number of 
sessions to be created within a test. Default value is 1.

range_read key is used to perform range read operations in a workload. 

part_range is range of numbers from which number of parts for multipart workloads can be 
calculated. 

Start and End range parameters are object/part sizes for which test will be executed.
Sizes can be given from bytes, KB, MB up to TB. We can also use KiB format as well. 

Result duration can be specified from seconds up to days. For example: 1d1h, 1h or 2d1h2s.

Tool can be specified from one of these **s3bench**, **s3api** or **warp**.

Operation should be specified according to type of workload and need to map it in corio.py with 
appropriate test script.

---

Sample YAML file can be found at **./sample_file.yaml**

YAML files for various workloads can be found at **corio/config/s3/s3api/**


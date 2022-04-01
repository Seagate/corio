YAML parser will receive a test yaml file as an input. 

A test yaml file should have the following parameters based on workloads to execute:

* TEST_ID
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

object_size can be given as fixed values, list of values and range of values for test 
workload, so those can have the following keys in it.
* start
* end

part_size can be given as fixed values and range of values for multipart workloads, so  those 
can have the following keys in it.
* start
* end 

sessions_per_node multiplied with number_of_nodes (command line argument '-nn') will be treated 
as total number of sessions for a test workload. Default value of number_of_nodes is 1.

sessions specified in YAML file will be directly used to create total sessions. 

range_read key is used to perform range read operations in a workload. 

part_range is range of numbers from which number of parts for multipart workloads can be 
calculated. 

Start and End range parameters are object/part sizes for which test will be executed.
Sizes can be given from bytes, KB (1000 Bytes), MB (1000 KB) up to TB (1000 GB). We can also use KiB (1024 Bytes) format as well.

min_runtime can be specified from seconds up to days. For example: 1d1h, 1h or 2d1h2s.

tool can be specified from one of these **s3bench**, **s3api** or **warp**.

operation should be specified according to type of workload and need to map it in corio.py with 
appropriate test script.

---

Sample YAML file can be found at **./sample_file.yaml**

YAML files for various workloads can be found at **corio/config/s3/s3api/**


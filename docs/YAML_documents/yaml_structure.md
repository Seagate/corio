YAML parser will receive a test yaml file as an input. 

A test yaml file should have the following parameters based on workloads to run:

* TEST_ID
* object_size
* part_size
* range_read
* part_range
* min_runtime
* sessions_per_node
* sessions
* tool
* operation
---

**TEST_ID** can be test ticket which is used for JIRA update or any unique string which is used to 
create log file for searching through run logs.  

**object_size** can be given as fixed values, list of values and range of values for test workload, 
Range for object size can be mentioned using start and end.
* start
* end

**part_size** can be given as fixed values and range of values for multipart workloads, so those 
can have the following keys in it.
* start
* end

Start and End range parameters are **object_size**/**part_size** for which test will run.
Sizes can be given from bytes, KB (1000 Bytes), MB (1000 KB) up to TB (1000 GB). We can also use 
KiB (1024 Bytes) format as well.

**sessions_per_node** multiplied with number_of_nodes (command line argument '-nn') will be treated 
as total number of sessions for a test workload. Default value of number_of_nodes is 1.

**sessions** specified in YAML file will be directly used to create total sessions. 

**range_read** key is used to perform range read operations in a workload. 

**part_range** is range of numbers from which random number of parts for multipart workloads can be 
calculated.

**min_runtime** can be specified from seconds up to days. For example: 1d1h, 1h or 2d1h2s.

**tool** can be specified from one of these **s3api**, **s3bench** or **warp**. 
    note:- s3bench and warp support to be added.

**operation** should be specified according to type of workload and need to map it in corio.py with 
appropriate test script.

---

Sample YAML file can be found at [sample file](sample_file.yaml)

YAML files for various s3 workloads can be found at **workload/s3/s3api/**


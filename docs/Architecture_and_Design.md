# CORIO

## OVERVIEW

### Block Diagram

![Block Diagram](images/BlockDiagram.png)

* **Workload Specification**: This is actual workload specified/created by user
* **Parser**: This section verifies the Workload Specification, Inputs and Structure used by Driver
* **Library**: This sections consists of collection of library used by Test Scripts
* **Test Scripts**: These are actual scripts which take structured input from Parser and are run by Driver
* **System Monitoring**: This consists of scripts for monitoring CPU and Memory usages on client and server
* **Logging**: Logs for individual tests in different files. These are stored to LOCAL/NFS as needed
* **Support Bundle**: This is a CORTX specific feature where Server Logs are generated periodically
on breakdown and available for debugging purposes
* **Health Check**: This is CORTX specific component checks the health of server (Status of Services)

#### IO Run Process Flow

![Flow Diagram](images/Flow_Diagram.png)

* IO Driver reads tests from TEST Plan
* Start executing System (CPU, Memory) Status Monitoring
* System/Services Health Check:
  * If system health or services status is not stable or down/irrevocable then will not proceed for IO run and
    will report for analysis
  * Space availability for IO (Need to stop IO if server disk space full)
  * If Space is full then IO will stop and  relevant logs will be collected for analysis.
* Start multiple workload configuration(tests) in parallel:  
  * Record the test thread/process for status monitoring
  * All threads(sessions) will continue run till terminates due to breakdown
* Wait for one of the conditions/tests stopping time
* If test stopped before given time due to any issue:
  * Report issue and Stop for debugging
* Check the Stop Conditions
* Mark test pass/fail (in JiraTest Plan)
* Collect Support Bundle irrespective of test status continuously
* If Test fails, it collects Support Bundle logs and upload to designated LOCAL/NFS (from server machine)
* Arrange/Collect and push Test/Tools o/p IO logs to designated LOCAL/NFS Server (from Client machine)
* Start next test if previous tests run successfully for defined minimum run time

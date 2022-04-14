## CORIO

### OVERVIEW:

#### Block Diagram:

![](images/BlockDiagram.png)

* **Workload Specification**: This is actual workload specified/created by user
* **Parser**: This section verifies the Workload Specification, Inputs and Structure used by Driver
* **Library**: This sections consists of collection of library used by Test Scripts
* **Test Scripts**: These are actual scripts which take structured input from Parser and are executed by Driver
* **System Monitoring**: This consists of scripts for monitoring CPU and Memory usages on client and server
* **Logging**: Logs for individual tests in different files. These are stored to LOCAL/NFS as needed
* **Support Bundle**: This is a CORTX specific feature where Server Logs are generated periodically and 
on failure and are available for debugging purposes
* **Health Check**: This is CORTX specific component checks the health of server (Status of Services)

#### IO Execution Process Flow:

![](images/Flow_Diagram.png)


- IO Driver reads tests from TEST Plan
- Start executing System (CPU, Memory) Status Monitoring
- System/Services Health Check:
    - If system health or services status is not stable or down/irrevocable then will not proceed for IO execution and 
    will report for analysis
    - Space availability for IO (Need to stop IO if server disk space full)
    - If Space is full (not available) then will stop(not proceed) with IO and  will collect relevant logs and 
      will do analysis.
- Start multiple workload configuration(tests) in parallel:  
    - Record the test thread/process for status monitoring
    - All threads(sessions) will continue run till terminates due to failure
- Wait for one of the failure conditions/Tests stopping time
- If test stopped before given time due to any failure:
    - Report Failure and Stop for debugging
- Check the Stop Conditions
- Mark test pass/fail (in JiraTest Plan) 
- Collect Support Bundle irrespective of test status continuously
- If Test fails, it collects Support Bundle logs and upload to designated LOCAL/NFS (from server machine)
- Arrange/Collect and push Test/Tools o/p IO logs to designated LOCAL/NFS Server (from Client machine)
- Start next test if previous tests executed successfully for defined minimum run time

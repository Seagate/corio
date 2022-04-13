## CORIO

### WHY CORIO:

Needed a single aggregator / umbrella tool which does following:
- Checks Sustainability of IO Operations for long duration and updates results based on expected execution runtime
- Support Stress (exponential workload) as well as longevity testing.
- Supports S3 IO operations using full capability of boto APIs.
- Benefits from unique capabilities of multiple IO tools (S3bench, WARP, etc.
- Supports failure mode testing over long runs.
- Supports plug-n-play for other protocols (NFS, SMB, Block-IO, etc).
- Supports easy deployment and enables various usage scenario across teams.

### CURRENT CAPABILITIES:

- Matrix Based Test Execution:
  - Parallel test execution from multiple test suites, 
    - for e.g. first test from each suite will start and will be marked pass once it achieves ETA duration of execution.
  - Then next set of tests will start and previously “pass” marked tests will also continue to run for background load generation.
- Seed based execution for easy reproduction of failure scenarios.
- Customized for CORTX requirements:
  - Automated execution using Jenkins framework.
  - Results update in Jira.
  - Periodic system health checks.
  - Resource monitoring (CPU, Memory, etc.).
  - Capture support bundle logs (periodic and on failure).



### OVERVIEW:

#### Block Diagram:

![](images/BlockDiagram.png)

* Test Configurations: This is actual workload specified/created by user.
* Parser: This section verifies the Test Configurations, Inputs and Structure used by Driver.
* Library: This sections consists of collection of code logic used by Test Scripts.
* Test Scripts: These are actual logic takes structured input from parser and executed by Driver.
* System Monitoring: This consists of monitoring CPU and Memory usages on client and server.
* Logging: Logs for individual tests in different files and upload to NFS as needed.
* Support Bundle: This is a CORTX specific feature where Server Logs are generated periodically and 
on failure, available for debugging purposes.
* Health Check: This section check the health of server (Status of Services).

#### IO Execution Process Flow:

![](images/Flow_Diagram.png)


- IO Driver will read tests from TEST Plan.
- Start executing System (CPU, Memory) Status Monitoring.
- System/Services Health Check:
    - If system health or services status is not stable or down/irrevocable then will not proceed for IO execution and 
    will report for analysis.
    - Space availability for IO (Need to stop IO if server disk space full).
    - If Space is full (not available) then will stop(not proceed) with IO and  will collect relevant logs and 
      will do analysis.
- Start multiple workload configuration(tests) in parallel:  
    - Record the test thread/process for status monitoring.
    - All threads(sessions) will continue run till terminates due to failure.
- Wait for one of the failure conditions/Tests stopping time.
- If test stopped before given time due to any failure:
    - Report Failure and Stop for debugging.
- Check the Stop Conditions.
- Mark test pass/fail (in JiraTest Plan) .
- Collect SB Logs every  irrespective of test status continuously.
- If Test failed collect SB logs and upload to designated NFS Server (from server machine).
- Arrange/Collect and push Test/Tools o/p IO logs to designated NFS Server (from Client machine).
- Start next test if previous tests executed successfully for defined minimum run time.

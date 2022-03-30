Why CORIO (KOR-I-O)

Need to check IO Stability: Sustainability of S3 Ops with IO for expected longer duration.

Need for a tool which gives consistent outcome of where do we stand in terms of IO Path stability.

This tool is capable to execute multi-level test suite that can be executed for a prolonged infinite period 
with increasing difficulty levels for them to Pass these set period, so we know the defects when fixed allows the 
system to withstand how much period of prolonged IO.
This tool generates exponential increasing IO sessions.
This Tool can run multiple tests suites in following manner.

Start Given Test Suites/Execution, say N in parallel such that first test from 
each suite will start and will be marked pass once it achieves minimum run_length duration.
Then next set of Tests will start and previous will also continue.
hence, it will increase load on target and, we can check IO sustainability in lesser time/duration  as compared to other
Tools.

This tool is capable of executing matrix based execution

Additional Capabilities:
    Simple and easy to execute tool.
    Jira Integration
    Automated Execution with Jenkins
    System Health Check and
    Generating Support Bundle Logs
    Plug, and play architecture such that additional functionalities may be extended
    Can be extended with other IO protocols

Components are as following:

Block Diagram ![img.png](Block_Diagram.png)

Flow Diagram ![img_2.png](Flow_Diagram.png)

IO Process Flow:
Process Flow (IO Driver)

IO Driver will read tests from TEST Plan

Start executing System (CPU, Memory) Status Monitoring.

System/Services Health Check.

    If system health or services status is not stable or down/irrevocable then will not proceed for IO execution and 

    will report for analysis

    Space availability for IO (Need to stop IO if server disk space full)

    If Space is full (not available) then will stop(not proceed) with IO and 

    will collect relevant logs and will do analysis

Start multiple workload configuration(tests) in parallel  

    will record the test thread/process for status monitoring.

    All threads(sessions) will continue running till terminates due to failure.

Wait for one of the failure conditions/Tests stopping time

If test stopped before given time due to any failure

    <Report Failure and continue as possible>

Categorize/Group  IO Failure.

    Report/Check the Health Check Status and stop further execution on node.

Check the Stop Conditions

Mark test pass/fail (in JiraTest Plan) 

    Collect SB Logs every  irrespective of test status continuously.
    If Test failed collect SB logs and upload to designated NFS Server (from server machine)
    Arrange/Collect and push Test/Tools o/p IO logs to designated NFS Server (from Client machine).

Start next test if previous tests executed successfully for defined minimum run time.
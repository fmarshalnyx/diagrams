Environment:
JAVA 1.8 is needed.

1. Lib contains the below jars and it is needs to be in the classpath when launching any Sample or APIclient.
   cfapi-core-1.0.X.X.jar is the Common feed API client core jar.
   cfapi-common-1.0.X.X.jar contains the common code and the interfaces, source for this jar is included in the distribution and it is available in sources dir.
   api-client-1.0.X.X.jar contains the codes for ClientSample, a sample client which demonstrates the usage of the cfapi client, source for this jar is included in the distribution and it is available in sources dir.
   api-testdump-1.0.X.X.jar contains the codes for QA TestDumpSample, 

2. sources folder contains the below jars with the source code.
   api-client-1.0.X.X-sources.jar contain the source code for the ClientSample which demonstrates the initialization and also on the usage of the Common Feed Api.
   cfapi-common-1.0.X.X-sources.jar contain the source code for the interfaces and the common code used by the sample programs.

3. Before launching the client make sure JAVA_HOME environment variable is set.

4. Create a logs folder under current directory for the Sample client program to generate application log(cfapilog).

5. input file contains the sample commands that can be passed to the client. Make sure the input file that is passed as option to the client is available in classpath, Default location is the current directory.

6. test.sh used to test the Common feed API client and it is primarily used by QA.
   test.sh a sample shell script to launch the TestDumpSample. This sample is in compliant to previous release(1.0) and it is testing the backward compatibility.
   test_V102X.sh is a sample shell script used to launch TestDumpSample1. This sample has the new changes in MyMessageEventReader while reading fields from MessageEvent. Use this sample to test all the new features added in 1.0.2.x

7. clienttest.sh used to demonstrate the Common feed API client usage.
   clienttest.sh a sample shell script to launch ClientSample. This sample is in compliant to previous release(1.0) and it is testing the backward compatibility.
   clienttest_V102X.sh is a sample shell script used to launch ClientSample1. This sample has the new changes in MyMessageEventReader while reading fields from MessageEvent. Use this sample to test all the new features added in 1.0.2.x
   
Both the client programs follow the same usage.
Usage: Sample program needs the following options to be passed.
./clienttest.sh [-h] [-q] [-D] [-o output file] -u userid -p password [-M] [-U max_user_threads] [-C max_csp_threads] [-z] [-r read timeout] [-s interval] [-c] [-Q queue size] remote_host_ip_address remote_port [backup_host_ip_address backup_port] input_command_file

8. clienttest.bat a sample batch file to launch ClientSample in windows environment.

Usage:
clienttest.bat [-h] [-q] [-D] [-o output file] [-u userid] [-p password] [-M] [-U max_user_threads] [-C max_csp_threads] [-z] [-r read timeout] [-s interval] [-c]  [-Q queue size]  remote_host_ip_address remote_port [backup_host_ip_address backup_port] input_command_file

9. If you want to coustomize the CFapi log file name and the location of the logs dir it can be done by setting the following system propeties.
Example:
	-Dcfapi.logs.dir=logs
	-Dcfapi.logfile=cfapilog
	
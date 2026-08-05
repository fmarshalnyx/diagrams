#  Usage: ClientSample [-h] [-q] [-D] [-o output file] -u userid -p password [-M] [-U max_user_threads] [-C max_csp_threads] [-z] [-c] [-r read timeout] [-s interval] [-c] [-Q queue size] [-I input-_req_q_size] [-a conflation_interval] [-n NAT_file] [-t conflation_type] [-j JIT_threshold] [-T queue_threshold] -P primary_host_ip_address primary_port [-B backup_host_ip_address backup_port] input_command_file
#  -h:             Prints this help message
#  -q:             Quiet mode - don't print data
#  -D:             Debug mode - print API debug messages to console
#  -o:             Output file - write data here instead of console
#  -u:             userid for CSP
#  -p:             password for CSP
#  -M:             Use Multithreading mode in API [One thread per TCP connection to CSP for better performnce]
#  -U:             Max user-side threads API may create when in Multithreading mode - [with -M option]
#  -C:             Max CSP-side threads API may create when in Multithreading mode - [with -M option]
#  -z:             Turn off compression between API and CSP
#  -r:             Read timeout in seconds
#  -s:             Get API statistics; specify interval in seconds
#  -c:             Request conflation indicator
#  -Q:             Queue size in megabytes		[Default 1 MB]
#  -I:             input request queue size	[Default 100000(requests)]
#  -a:             Conflation interval in milliseconds. Default is 1000 (ms)
#  -n:             NAT IP address mapping file (each line should contain <CSP IP addr>,<local IP addr>)
#  -t:             Conflation type. 1=Trade Safe(default); 2=Intervalized; 3=Just-In-Time (JIT); 4=Just-In-Time(JIT) Intervalized
#  -j:             JIT Conflation threshold. Buffer percent full to trigger conflation. 1-75 Default is 25. Only valid with -t3 or -t4
#  -w:             Use watchlist. [Default is false]
#  -W:             Max watchlist size. [Default 10000000(requests)]
#  -T:             Queue threshold. Buffer percent full to trigger CFAPI_SESSION_RECEIVE_QUEUE_ABOVE_THRESHOLD and CFAPI_SESSION_RECEIVE_QUEUE_BELOW_THRESHOLD SessionEvents. 1-101 Default is 70%
#  -P:				Primary connection  with <IP addr><Port no>
#  -B: 			Backup connection  with <IP addr><Port no>
#  input_command_file:  file containing CTF commands

userid=usbankTrial
password=usbankTrial1
primary_host_ip_address=216.221.209.61
primary_port=7022
backup_host_ip_address=216.221.209.62
backup_port=7022
input_command_file=commands.txt

set -x
./clienttest.sh -u $userid -p $password -P $primary_host_ip_address $primary_port -B $backup_host_ip_address $backup_port $input_command_file

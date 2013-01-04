"""

Usage is:

ngamsServer -cfg <Cfg File> [-dbCfgId <Cfg ID>]
            [-v <Level>] [-version] [-license]
            [-locLogFile <Log File>] [-locLogLevel <Level>]
            [-sysLog <Level>] [-sysLogPrefix <Prefix>]
            [-force] [-autoOnline] [-noAutoExit] [-multipleSrvs] [-d]
            [-test]

The command line parameters are:

  -cfg <Cfg File>:         NG/AMS Configuration File.

  -dbCfgId <Cfg ID>:       Configuration ID of configuration specified in the
                           DB. The only parameters necessary in the 
                           configuration specified in connection with the
                           -cfg option, is the DB connection properties.

  -v <Level>:              Verbose Mode + Level.

  -version:                Print out version of server.

  -license:                Print out license information.

  -locLogFile <File>:      Name of Local Log File.

  -locLogLevel <Level>:    Level for logging in Local Log File.

  -sysLog <Level>:         Switch syslog logging on.

  -sysLogPrefix <Prefix>:  Prefix for syslog logging.

  -force:                  Force execution eventhough PID File found.

  -autoOnline:             Bring the server to Online State automatically 
                           after initialization.

  -noAutoExit              If -autoOnline is specified and an error occurs 
                           preventing the system from going Online, it will not
                           automatically exit.

  -multipleSrvs:           If specified, it will be possible to run multiple
                           servers on one node. In this mode, the Host ID in
                           the NGAS Hosts DB table is "<Host Name>:<Port No>" 
                           rather than just the hostname.

  -d:                      Debugging Mode.

  -test:                   Flag used in connection with the NG/AMS Unit Tests.

Note: The values given on the command line, overwrites the ones given in 
the NG/AMS Configuration File.

"""

# EOF

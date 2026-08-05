@echo off

SETLOCAL ENABLEDELAYEDEXPANSION

if defined JAVA_HOME (
 echo JAVA_HOME is defined as !JAVA_HOME!
)else (
    echo Need to setup JAVA_HOME for the client to start
    exit /b
)
 

set SCRIPT_DIR=%cd%
set APP_DIR=%SCRIPT_DIR%
rem echo %SCRIPT_DIR%
rem echo %APP_DIR%

if exist %APP_DIR%/logs (
    echo logs folder exists
) else (
    echo creating logs folder
    mkdir  %APP_DIR%\logs
)

For /f "tokens=2-4 delims=/ " %%a in ('date /t') do (set mydate=%%c%%a%%b)
For /f "tokens=1-2 delims=/:" %%a in ("%TIME%") do (set mytime=%%a%%b)

rem set mydate=20190307
rem set mytime= 941

set mydatetime=%mydate%-%mytime%
set mydatetime=%mydatetime: =%

SETLOCAL DisableDelayedExpansion

SET TODAYS_DATE=%mydate%
SET GC_FLAGS=-XX:MetaspaceSize=100M
SET GC_FLAGS=%GC_FLAGS% -XX:+UseConcMarkSweepGC -XX:-CMSConcurrentMTEnabled -XX:CMSInitiatingOccupancyFraction=60
SET GC_FLAGS=%GC_FLAGS% -XX:+DisableExplicitGC -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+PrintGCDetails
SET GC_FLAGS=%GC_FLAGS% -verbose:gc -Xloggc:.\logs\%mydatetime%-cfapi_gc.log
SET GC_FLAGS=%GC_FLAGS% -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=%APP_DIR%\logs
rem SET OOM_JVM_PARAM=-XX:OnOutOfMemoryError=kill -9 %p

SET DEBUG=-Xdebug -Xrunjdwp:transport=dt_socket,address=5005,server=y,suspend=n
rem #DEBUG="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"

SET JAVA=%JAVA_HOME%\bin\java

SET CLASS_PATH=%APP_DIR%;%APP_DIR%\lib\*
SET LD_LIBRARY_PATH=${LD_LIBRARY_PATH};%APP_DIR%\bin
rem SET LIB_DIRS=%APP_DIR%\lib\parent;%APP_DIR%\lib;%JAVA_HOME%\jre\lib\ext

SET GC_FLAGS=%GC_FLAGS% -Xms1G -Xmx1G

SET MAIN_CLASS=com.theice.cfapi.client.example.ClientSample
rem SET MAIN_CLASS=com.theice.cfapi.test.example.ClientSample

rem echo %JAVA%
rem echo %GC_FLAGS%

SETLOCAL ENABLEDELAYEDEXPANSION

rem #-M -U 2 -C 4 -D
rem no adding "%OOM_JVM_PARAM%"

rem SET COMMAND="%JAVA%" %DEBUG% %GC_FLAGS% -cp "%CLASS_PATH%" -Dconnection.timeout=60 %MAIN_CLASS% %* ^>%APP_DIR%\logs\idcapi-%mydatetime%.out
rem #To change the connection timeout add the below JVM property.
rem SET COMMAND="%JAVA%" %DEBUG% %GC_FLAGS% -cp "%CLASS_PATH%" -Djava.library.path=%APP_DIR%\bin -Djna.library.path=%APP_DIR%\bin -Dconnection.timeout=60 %MAIN_CLASS% %*
SET COMMAND="%JAVA%" %DEBUG% %GC_FLAGS% -cp "%CLASS_PATH%" -Djava.library.path=%APP_DIR%\bin -Djna.library.path=%APP_DIR%\bin %MAIN_CLASS% %*

echo !COMMAND!
%COMMAND%

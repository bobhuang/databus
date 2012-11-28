@REM ----------------------------------------------------------------------------
@REM Copyright 2001-2004 The Apache Software Foundation.
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM ----------------------------------------------------------------------------
@REM

@echo off

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of arguments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto repoSetup

:WinNTGetScriptDir
set BASEDIR=%~dp0\..

:repoSetup


if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\repo

set CLASSPATH="%BASEDIR%"\conf;"%REPO%"\log4j\log4j\1.2.15\log4j-1.2.15.jar;"%REPO%"\org\apache\zookeeper\zookeeper\3.3.3\zookeeper-3.3.3.jar;"%REPO%"\jline\jline\0.9.94\jline-0.9.94.jar;"%REPO%"\com\thoughtworks\xstream\xstream\1.3.1\xstream-1.3.1.jar;"%REPO%"\xpp3\xpp3_min\1.1.4c\xpp3_min-1.1.4c.jar;"%REPO%"\org\codehaus\jackson\jackson-core-asl\1.8.5\jackson-core-asl-1.8.5.jar;"%REPO%"\org\codehaus\jackson\jackson-mapper-asl\1.8.5\jackson-mapper-asl-1.8.5.jar;"%REPO%"\commons-io\commons-io\1.4\commons-io-1.4.jar;"%REPO%"\commons-cli\commons-cli\1.2\commons-cli-1.2.jar;"%REPO%"\com\github\sgroschupf\zkclient\0.1\zkclient-0.1.jar;"%REPO%"\org\apache\camel\camel-josql\2.5.0\camel-josql-2.5.0.jar;"%REPO%"\org\apache\camel\camel-core\2.5.0\camel-core-2.5.0.jar;"%REPO%"\commons-logging\commons-logging-api\1.1\commons-logging-api-1.1.jar;"%REPO%"\org\fusesource\commonman\commons-management\1.0\commons-management-1.0.jar;"%REPO%"\net\sf\josql\josql\1.5\josql-1.5.jar;"%REPO%"\net\sf\josql\gentlyweb-utils\1.5\gentlyweb-utils-1.5.jar;"%REPO%"\org\apache\commons\commons-math\2.1\commons-math-2.1.jar;"%REPO%"\org\restlet\org.restlet\1.1.10\org.restlet-1.1.10.jar;"%REPO%"\com\noelios\restlet\com.noelios.restlet\1.1.10\com.noelios.restlet-1.1.10.jar;"%REPO%"\com\linkedin\helix\helix-core\0.5.24\helix-core-0.5.24.jar
set EXTRA_JVM_ARGUMENTS=-Xms512m -Xmx512m
goto endInit

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS% %EXTRA_JVM_ARGUMENTS% -classpath %CLASSPATH_PREFIX%;%CLASSPATH% -Dapp.name="helix-admin" -Dapp.repo="%REPO%" -Dbasedir="%BASEDIR%" com.linkedin.helix.tools.ClusterSetup %CMD_LINE_ARGS%
if ERRORLEVEL 1 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=1

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@endlocal

:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /B %ERROR_CODE%

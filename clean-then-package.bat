@echo off
setlocal

set JAVA_HOME=C:\Program Files\Java\jdk1.8.0_192

set IDEA_HOME=C:\Program Files\JetBrains\IntelliJ IDEA Community Edition

set MAVEN_HOME=%IDEA_HOME%\plugins\maven\lib\maven3

set WORK_DIR=C:\Users\Administrator\Desktop\企业计算\calc-engine

"%JAVA_HOME%\bin\java.exe"^
    -Dmaven.multiModuleProjectDirectory=%WORK_DIR%^
    "-Dmaven.home=%MAVEN_HOME%"^
    "-Dclassworlds.conf=%IDEA_HOME%\plugins\maven\lib\maven3\bin\m2.conf"^
    -Dfile.encoding=UTF-8^
    -classpath "%MAVEN_HOME%\boot\plexus-classworlds-2.5.2.jar"^
    org.codehaus.classworlds.Launcher^
    clean package

endlocal
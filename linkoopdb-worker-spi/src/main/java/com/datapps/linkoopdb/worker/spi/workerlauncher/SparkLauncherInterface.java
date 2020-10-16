package com.datapps.linkoopdb.worker.spi.workerlauncher;

import java.io.File;
import java.util.Map;

public interface SparkLauncherInterface {

    void initSparkLauncher(Map<String, String> env);

    void setSparkHome(String sparkHome);

    void addSparkArg(String argName, String argValue);

    void setAppResource(String appResource);

    void setMainClass(String mainClass);

    void addAppArgs(String... appArgs);

    void setMaster(String master);

    void setDeployMode(String deployMode);

    void setConf(String confName, String confValue);

    void setVerbose(boolean verbose);

    int startApplication(SparkLauncherListenerMethod method1, SparkLauncherListenerMethod method2);

    void setAppName(String appName);

    void addJar(String jar);

    void setPropertiesFile(String path);

    void addFile(String file);

    void redirectToLog(String loggerName);

    void redirectOutput(File file);

    String getAppId();
}

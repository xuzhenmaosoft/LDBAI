package com.datapps.linkoopdb.worker.spi.workerlauncher;

@FunctionalInterface
public interface SparkLauncherListenerMethod {

    void doSometing(boolean isFinal, String message);
}

package com.mlesniak.spark;

import com.mlesniak.runner.RunnerConfiguration;

public class Configuration extends RunnerConfiguration {
    private String directory;
    
    public String getDirectory() {
        return directory;
    }

    public static Configuration get() {
        return (Configuration) RunnerConfiguration.get();
    }
}

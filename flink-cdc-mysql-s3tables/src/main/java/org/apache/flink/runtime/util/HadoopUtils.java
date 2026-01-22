package org.apache.flink.runtime.util;

import org.apache.hadoop.conf.Configuration;

public class HadoopUtils {

    public static Configuration getHadoopConfiguration(
            org.apache.flink.configuration.Configuration flinkConfiguration) {
        return new Configuration(false);
    }
}

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class HadoopUtilsTest {

    @Test
    void getHadoopConfigurationShouldReturnNonNullConfiguration() {
        Configuration flinkConfig = new Configuration();

        org.apache.hadoop.conf.Configuration hadoopConfig = HadoopUtils.getHadoopConfiguration(flinkConfig);

        assertNotNull(hadoopConfig);
    }
}

package com.amazonaws.java.flink.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CDCRecordsTest {

    @Test
    void gettersShouldReturnConstructorValues() {
        CDCRecords record = new CDCRecords(
                "before", "after", "source", "c", "1234567890", "tx123"
        );

        assertEquals("before", record.getBefore());
        assertEquals("after", record.getAfter());
        assertEquals("source", record.getSource());
        assertEquals("c", record.getOp());
        assertEquals("1234567890", record.getTs_ms());
        assertEquals("tx123", record.getTransaction());
    }
}

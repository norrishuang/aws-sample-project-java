package com.amazonaws.java.flink;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CDCDynamicRecordGeneratorTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void decimalValueReadsPlainDecimalText() throws Exception {
        assertEquals("123.45",
                CDCDynamicRecordGenerator.decimalValue(
                        objectMapper.readTree("\"123.45\""), 2).toPlainString());
    }

    @Test
    void decimalValueDecodesBase64UnscaledValue() throws Exception {
        String encoded = Base64.getEncoder().encodeToString(
                BigInteger.valueOf(12345).toByteArray());

        assertEquals("123.45",
                CDCDynamicRecordGenerator.decimalValue(
                        objectMapper.readTree("\"" + encoded + "\""), 2).toPlainString());
    }

    @Test
    void decimalValueUsesVariableScaleDecimalEnvelope() throws Exception {
        String encoded = Base64.getEncoder().encodeToString(
                BigInteger.valueOf(-12345).toByteArray());

        assertEquals("-12.345",
                CDCDynamicRecordGenerator.decimalValue(
                        objectMapper.readTree("{\"scale\":3,\"value\":\"" + encoded + "\"}"), 2)
                        .toPlainString());
    }

    @Test
    void bytesWithDecimalScaleMapsToIcebergDecimal() throws Exception {
        CDCDynamicRecordGenerator generator =
                new CDCDynamicRecordGenerator("default", null, false, 1);

        assertEquals(Types.DecimalType.of(12, 2),
                generator.mapDebeziumType(
                        "bytes",
                        null,
                        objectMapper.readTree(
                                "{\"scale\":\"2\",\"connect.decimal.precision\":\"12\"}")));
    }
}

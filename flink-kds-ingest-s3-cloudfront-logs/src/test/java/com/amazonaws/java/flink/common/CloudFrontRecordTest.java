package com.amazonaws.java.flink.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CloudFrontRecordTest {

    @Test
    void constructorAndGettersShouldReturnAssignedValues() {
        CloudFrontRecord record = new CloudFrontRecord(
                1.2f,
                "client-ip",
                "server-ip",
                3L,
                200,
                1024,
                "GET",
                "https",
                "example.com",
                "/index.html",
                2048,
                "EDGE",
                "request-id",
                "host-header",
                4L,
                "HTTP/1.1",
                "IPv4",
                "agent",
                "referer",
                "cookie",
                "query",
                "result",
                "forwarded",
                "TLSv1.2",
                "AES",
                "edge-result",
                "encrypted",
                "status",
                "content-type",
                4096,
                0,
                1024,
                443,
                "detailed",
                "US",
                "gzip",
                "accept",
                "/behaviour",
                "headers",
                "header-names",
                10
        );

        assertEquals(1.2f, record.getTimestamp());
        assertEquals("client-ip", record.getCip());
        assertEquals("server-ip", record.getSip());
        assertEquals(3L, record.getTimeToFirstByte());
        assertEquals(200, record.getScStatus());
        assertEquals(1024, record.getScBytes());
        assertEquals("GET", record.getCsMethod());
        assertEquals("https", record.getCsProto());
        assertEquals("example.com", record.getCsHost());
        assertEquals("/index.html", record.getCsUrl());
        assertEquals(2048, record.getCsBytes());
        assertEquals("EDGE", record.getXEdgeLocation());
        assertEquals("request-id", record.getXEdgeRequestId());
        assertEquals("host-header", record.getXHostHeader());
        assertEquals(4.0f, record.getTimeTaken());
        assertEquals("HTTP/1.1", record.getCsVersion());
        assertEquals("IPv4", record.getCipVersion());
        assertEquals("agent", record.getCsUserAgent());
        assertEquals("referer", record.getCsRefer());
        assertEquals("cookie", record.getCsCookie());
        assertEquals("query", record.getCsUriQuery());
        assertEquals("result", record.getXEdgeResponseResultType());
        assertEquals("forwarded", record.getXForwardedFor());
        assertEquals("TLSv1.2", record.getSslProtocol());
        assertEquals("AES", record.getSslCipher());
        assertEquals("edge-result", record.getXEdgeResultType());
        assertEquals("encrypted", record.getFleEncryptedFields());
        assertEquals("status", record.getFleStatus());
        assertEquals("content-type", record.getScContentType());
        assertEquals(4096, record.getScContentLen());
        assertEquals(0, record.getScRangeStart());
        assertEquals(1024, record.getScRangeEnd());
        assertEquals(443, record.getCPort());
        assertEquals("detailed", record.getXEdgeDetailedResultType());
        assertEquals("US", record.getCipCountry());
        assertEquals("gzip", record.getCsAcceptEncoding());
        assertEquals("accept", record.getCsAccept());
        assertEquals("/behaviour", record.getCacheBehaviorPathPattern());
        assertEquals("headers", record.getCsHeaders());
        assertEquals("header-names", record.getCsHeaderNames());
        assertEquals(10, record.getCsHeadersCount());
    }

    @Test
    void settersShouldUpdateValues() {
        CloudFrontRecord record = new CloudFrontRecord();
        record.setCip("client-ip");
        record.setScStatus(404);
        record.setCsUrl("/notfound");

        assertEquals("client-ip", record.getCip());
        assertEquals(404, record.getScStatus());
        assertEquals("/notfound", record.getCsUrl());
    }
}

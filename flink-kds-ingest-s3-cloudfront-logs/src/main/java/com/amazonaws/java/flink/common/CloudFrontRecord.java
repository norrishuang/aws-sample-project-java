package com.amazonaws.java.flink.common;

public class CloudFrontRecord {
    private float timestamp;
    private String cip;
    private String sip;
    private float timeToFirstByte;
    private int scStatus;
    private int scBytes;
    private String csMethod;
    private String csProto;
    private String csHost;
    private String csUrl;
    private int csBytes;
    private String xEdgeLocation;
    private String xEdgeRequestId;
    private String xHostHeader;
    private float timeTaken;
    private String csVersion;
    private String cipVersion;
    private String csUserAgent;
    private String csRefer;
    private String csCookie;
    private String csUriQuery;
    private String xEdgeResponseResultType;
    private String xForwardedFor;
    private String sslProtocol;
    private String sslCipher;
    private String xEdgeResultType;
    private String fleEncryptedFields;
    private String fleStatus;
    private String scContentType;
    private int scContentLen;
    private int scRangeStart;
    private int scRangeEnd;
    private int cPort;
    private String xEdgeDetailedResultType;
    private String cipCountry;
    private String csAcceptEncoding;
    private String csAccept;
    private String cacheBehaviorPathPattern;
    private String csHeaders;
    private String csHeaderNames;
    private int csHeadersCount;

    public CloudFrontRecord(){}

    public CloudFrontRecord(
      float timestamp,
      String cip,
      String sip,
      long timeToFirstByte,
      int scStatus,
      int scBytes,
      String csMethod,
      String csProto,
      String csHost,
      String csUrl,
      int csBytes,
      String xEdgeLocation,
      String xEdgeRequestId,
      String xHostHeader,
      long timeTaken,
      String csVersion,
      String cipVersion,
      String csUserAgent,
      String csRefer,
      String csCookie,
      String csUriQuery,
      String xEdgeResponseResultType,
      String xForwardedFor,
      String sslProtocol,
      String sslCipher,
      String xEdgeResultType,
      String fleEncryptedFields,
      String fleStatus,
      String scContentType,
      int scContentLen,
      int scRangeStart,
      int scRangeEnd,
      int cPort,
      String xEdgeDetailedResultType,
      String cipCountry,
      String csAcceptEncoding,
      String csAccept,
      String cacheBehaviorPathPattern,
      String csHeaders,
      String csHeaderNames,
      int csHeadersCount) {
        this.timestamp = timestamp;
        this.cip = cip;
        this.sip = sip;
        this.timeToFirstByte = timeToFirstByte;
        this.scStatus = scStatus;
        this.scBytes = scBytes;
        this.csMethod = csMethod;
        this.csProto = csProto;
        this.csHost = csHost;
        this.csUrl = csUrl;
        this.csBytes = csBytes;
        this.xEdgeLocation = xEdgeLocation;
        this.xEdgeRequestId = xEdgeRequestId;
        this.xHostHeader = xHostHeader;
        this.timeTaken = timeTaken;
        this.csVersion = csVersion;
        this.cipVersion = cipVersion;
        this.csUserAgent = csUserAgent;
        this.csRefer = csRefer;
        this.csCookie = csCookie;
        this.csUriQuery = csUriQuery;
        this.xEdgeResponseResultType = xEdgeResponseResultType;
        this.xForwardedFor = xForwardedFor;
        this.sslProtocol = sslProtocol;
        this.sslCipher = sslCipher;
        this.xEdgeResultType = xEdgeResultType;
        this.fleEncryptedFields = fleEncryptedFields;
        this.fleStatus = fleStatus;
        this.scContentType = scContentType;
        this.scContentLen = scContentLen;
        this.scRangeStart = scRangeStart;
        this.scRangeEnd = scRangeEnd;
        this.cPort = cPort;
        this.xEdgeDetailedResultType = xEdgeDetailedResultType;
    }
    // Setters
    public void setTimestamp(float timestamp) {
        this.timestamp = timestamp;
    }

    public void setCip(String cip) {
        this.cip = cip;
    }

    public void setSip(String sip) {
        this.sip = sip;
    }

    public void setTimeToFirstByte(float timeToFirstByte) {
        this.timeToFirstByte = timeToFirstByte;
    }

    public void setScStatus(int scStatus) {
        this.scStatus = scStatus;
    }

    public void setScBytes(int scBytes) {
        this.scBytes = scBytes;
    }

    public void setCsMethod(String csMethod) {
        this.csMethod = csMethod;
    }

    public void setCsProto(String csProto) {
        this.csProto = csProto;
    }

    public void setCsHost(String csHost) {
        this.csHost = csHost;
    }

    public void setCsUrl(String csUrl) {
        this.csUrl = csUrl;
    }

    public void setCsBytes(int csBytes) {
        this.csBytes = csBytes;
    }

    public void setXEdgeLocation(String xEdgeLocation) {
        this.xEdgeLocation = xEdgeLocation;
    }

    public void setXEdgeRequestId(String xEdgeRequestId) {
        this.xEdgeRequestId = xEdgeRequestId;
    }

    public void setXHostHeader(String xHostHeader) {
        this.xHostHeader = xHostHeader;
    }

    public void setTimeTaken(float timeTaken) {
        this.timeTaken = timeTaken;
    }

    public void setCsVersion(String csVersion) {
        this.csVersion = csVersion;
    }

    public void setCipVersion(String cipVersion) {
        this.cipVersion = cipVersion;
    }

    public void setCsUserAgent(String csUserAgent) {
        this.csUserAgent = csUserAgent;
    }

    public void setCsRefer(String csRefer) {
        this.csRefer = csRefer;
    }

    public void setCsCookie(String csCookie) {
        this.csCookie = csCookie;
    }

    public void setCsUriQuery(String csUriQuery) {
        this.csUriQuery = csUriQuery;
    }

    public void setXEdgeResponseResultType(String xEdgeResponseResultType) {
        this.xEdgeResponseResultType = xEdgeResponseResultType;
    }

    public void setXForwardedFor(String xForwardedFor) {
        this.xForwardedFor = xForwardedFor;
    }

    public void setSslProtocol(String sslProtocol) {
        this.sslProtocol = sslProtocol;
    }

    public void setSslCipher(String sslCipher) {
        this.sslCipher = sslCipher;
    }

    public void setXEdgeResultType(String xEdgeResultType) {
        this.xEdgeResultType = xEdgeResultType;
    }

    public void setFleEncryptedFields(String fleEncryptedFields) {
        this.fleEncryptedFields = fleEncryptedFields;
    }

    public void setFleStatus(String fleStatus) {
        this.fleStatus = fleStatus;
    }

    public void setScContentType(String scContentType) {
        this.scContentType = scContentType;
    }

    public void setScContentLen(int scContentLen) {
        this.scContentLen = scContentLen;
    }

    public void setScRangeStart(int scRangeStart) {
        this.scRangeStart = scRangeStart;
    }

    public void setScRangeEnd(int scRangeEnd) {
        this.scRangeEnd = scRangeEnd;
    }

    public void setCPort(int cPort) {
        this.cPort = cPort;
    }

    public void setXEdgeDetailedResultType(String xEdgeDetailedResultType) {
        this.xEdgeDetailedResultType = xEdgeDetailedResultType;
    }

    public void setCipCountry(String cipCountry) {
        this.cipCountry = cipCountry;
    }

    public void setCsAcceptEncoding(String csAcceptEncoding) {
        this.csAcceptEncoding = csAcceptEncoding;
    }

    public void setCsAccept(String csAccept) {
        this.csAccept = csAccept;
    }

    public void setCacheBehaviorPathPattern(String cacheBehaviorPathPattern) {
        this.cacheBehaviorPathPattern = cacheBehaviorPathPattern;
    }

    public void setCsHeaders(String csHeaders) {
        this.csHeaders = csHeaders;
    }

    public void setCsHeaderNames(String csHeaderNames) {
        this.csHeaderNames = csHeaderNames;
    }

    public void setCsHeadersCount(int csHeadersCount) {
        this.csHeadersCount = csHeadersCount;
    }


    // Getters
    public float getTimestamp() {
        return this.timestamp;
    }

    public String getCip() {
        return this.cip;
    }

    public String getSip() {
        return this.sip;
    }

    public float getTimeToFirstByte() {
        return timeToFirstByte;
    }

    public int getScStatus() {
        return scStatus;
    }

    public int getScBytes() {
        return scBytes;
    }

    public String getCsMethod() {
        return csMethod;
    }

    public String getCsProto() {
        return csProto;
    }

    public String getCsHost() {
        return csHost;
    }

    public String getCsUrl() {
        return csUrl;
    }

    public int getCsBytes() {
        return csBytes;
    }

    public String getXEdgeLocation() {
        return xEdgeLocation;
    }

    public String getXEdgeRequestId() {
        return xEdgeRequestId;
    }

    public String getXHostHeader() {
        return xHostHeader;
    }

    public float getTimeTaken() {
        return timeTaken;
    }

    public String getCsVersion() {
        return csVersion;
    }

    public String getCipVersion() {
        return cipVersion;
    }

    public String getCsUserAgent() {
        return csUserAgent;
    }

    public String getCsRefer() {
        return csRefer;
    }

    public String getCsCookie() {
        return csCookie;
    }

    public String getCsUriQuery() {
        return csUriQuery;
    }

    public String getXEdgeResponseResultType() {
        return xEdgeResponseResultType;
    }

    public String getXForwardedFor() {
        return xForwardedFor;
    }

    public String getSslProtocol() {
        return sslProtocol;
    }

    public String getSslCipher() {
        return sslCipher;
    }

    public String getXEdgeResultType() {
        return xEdgeResultType;
    }

    public String getFleEncryptedFields() {
        return fleEncryptedFields;
    }

    public String getFleStatus() {
        return fleStatus;
    }

    public String getScContentType() {
        return scContentType;
    }

    public int getScContentLen() {
        return scContentLen;
    }

    public int getScRangeStart() {
        return scRangeStart;
    }

    public int getScRangeEnd() {
        return scRangeEnd;
    }

    public int getCPort() {
        return cPort;
    }

    public String getXEdgeDetailedResultType() {
        return xEdgeDetailedResultType;
    }

    public String getCipCountry() {
        return cipCountry;
    }

    public String getCsAcceptEncoding() {
        return csAcceptEncoding;
    }

    public String getCsAccept() {
        return csAccept;
    }

    public String getCacheBehaviorPathPattern() {
        return cacheBehaviorPathPattern;
    }

    public String getCsHeaders() {
        return csHeaders;
    }

    public String getCsHeaderNames() {
        return csHeaderNames;
    }

    public int getCsHeadersCount() {
        return csHeadersCount;
    }
}
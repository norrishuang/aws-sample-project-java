package com.amazonaws.java.flink.common;

public class CDCRecords {
    private String before;
    private String after;
    private String source;
    private String op;
    private String ts_ms;
    private String transaction;


    public CDCRecords(String before, String after, String source,String op, String ts_ms, String transaction) {
        this.before = before;
        this.after = after;
        this.source = source;
        this.op = op;
        this.ts_ms = ts_ms;
        this.transaction = transaction;
    }

    public String getBefore() {
        return before;
    }

    public String getAfter () {
        return after;
    }

    public String getSource() {
        return source;
    }

    public String getOp() {
        return op;
    }

    public String getTs_ms() {
        return ts_ms;
    }

    public String getTransaction() {
        return transaction;
    }


}

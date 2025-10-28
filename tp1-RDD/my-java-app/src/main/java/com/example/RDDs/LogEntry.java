package com.example.RDDs;

public class LogEntry {
    String ip;
    String dateTime;
    String method;
    String resource;
    int code;
    int size;

    public LogEntry(String ip, String dateTime, String method, String resource, int code, int size) {
        this.ip = ip;
        this.dateTime = dateTime;
        this.method = method;
        this.resource = resource;
        this.code = code;
        this.size = size;
    }
}

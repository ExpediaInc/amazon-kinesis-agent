package com.expedia.www.haystack.expweb.log.transformer;

public enum TraceTags {
    CLIENT("client"),
    TRANSACTION_TYPE("transactiontype"),
    EVENT_NAME_KEY("eventname"),
    TRACE_ID("traceid"),
    MESSAGE_ID("messageid"),
    PARENT_MESSAGE_ID("parentmessageid"),
    EVENT_TIME("eventtime"),
    DURATION("duration"),
    CLIENT_IP("clientip");

    private final String key;

    TraceTags(String key) {
        this.key = key;
    }

    public String getKey() {
        return this.key;
    }
}

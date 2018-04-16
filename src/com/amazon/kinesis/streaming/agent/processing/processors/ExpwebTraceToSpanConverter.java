package com.amazon.kinesis.streaming.agent.processing.processors;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;
import com.expedia.open.tracing.Log;
import com.expedia.open.tracing.Span;
import com.expedia.open.tracing.Tag;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This converter converts a expweb trace log entry into a span for haystack.
 */
public class ExpwebTraceToSpanConverter implements IDataConverter {

    enum TraceTags {
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

    @Override
    public ByteBuffer convert(ByteBuffer data) {
        final String record = ByteBuffers.toString(data, StandardCharsets.UTF_8);
        final Map<String, String> recordMap = splitRecord(record);
        Span span = createSpan(recordMap);
        return ByteBuffer.wrap(span.toByteArray());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    private static final String CLIENT_NAME = "expweb";
    private static final String SUCCESS_KEY = "success";
    private static final String ERROR_KEY = "error";
    private static final String KV_REGEX = "(\\w+)=(\"([^\"]*)\"|([^,]*))";

    private final Pattern kvPattern = Pattern.compile(KV_REGEX);

    private Map<String, String> splitRecord(String record) {
        final Map<String, String> kvPairs = new HashMap<>();
        final Matcher matcher = kvPattern.matcher(record);
        while (matcher.find()) {
            if (matcher.groupCount() == 4) {
                final String key = matcher.group(1);
                if (key != null) {
                    final String value = matcher.group(3) == null ? matcher.group(2) : matcher.group(3);
                    kvPairs.put(isTraceTag(key.toLowerCase(Locale.ENGLISH)) ? key.toLowerCase(Locale.ENGLISH) : key,
                            value == null ? "" : value);
                }
            }
        }
        return kvPairs;
    }


    private Span createSpan(Map<String, String> recordMap) {

        final String messageId = recordMap.get(TraceTags.MESSAGE_ID.getKey());
        final String traceId = recordMap.get(TraceTags.TRACE_ID.getKey());
        final String parentMessageId = recordMap.get(TraceTags.PARENT_MESSAGE_ID.getKey());
        final long eventTime = Long.parseLong(recordMap.get(TraceTags.EVENT_TIME.getKey()));
        final long duration = Long.parseLong(recordMap.get(TraceTags.DURATION.getKey()));
        final String transactionType = getOrDefault(recordMap, TraceTags.TRANSACTION_TYPE.getKey(), "");
        final long startTime = eventTime - duration;

        final Span.Builder spanBuilder = Span.newBuilder()
                .setServiceName(CLIENT_NAME)
                .setTraceId(traceId)
                .setSpanId(messageId)
                .setOperationName(getOrDefault(recordMap, TraceTags.EVENT_NAME_KEY.getKey(), ""))
                .setStartTime(startTime * 1000)
                .setDuration(duration * 1000)
                .addTags(Tag.newBuilder().setType(Tag.TagType.STRING).setKey("clientVersion").setVStr(getOrDefault
                        (recordMap, TraceTags.CLIENT.getKey(), "")).build())
                .addTags(Tag.newBuilder().setType(Tag.TagType.STRING).setKey("hostIP").setVStr(getOrDefault
                        (recordMap, TraceTags.CLIENT_IP.getKey(), "")).build())
                .addAllTags(getContextList(recordMap))
                .addAllLogs(getTransactionTypeLogs(transactionType, startTime, eventTime));


        if(StringUtils.isNotEmpty(parentMessageId)){
            spanBuilder.setParentSpanId(parentMessageId);
        }

        return spanBuilder.build();
    }

    private String getOrDefault(Map<String, String> recordMap, String key, String defaultVal) {
        final String value = recordMap.get(key);
        return (value != null)? value : defaultVal;

    }

    private List<Log> getTransactionTypeLogs(String transactionType, long startTime, long endTime) {
        final List<Log> transactionTypeLogs = new ArrayList<>();
        final long startTimeInMicroSec = startTime * 1000;
        final long endTimeInMicroSec = endTime * 1000;
        if ("server".equalsIgnoreCase(transactionType)) {
            transactionTypeLogs.add(Log.newBuilder().setTimestamp(startTimeInMicroSec).addFields(Tag.newBuilder().setKey("event").setVStr("sr")).build());
            transactionTypeLogs.add(Log.newBuilder().setTimestamp(endTimeInMicroSec).addFields(Tag.newBuilder().setKey("event").setVStr("ss")).build());
        } else if ("client".equalsIgnoreCase(transactionType)) {
            transactionTypeLogs.add(Log.newBuilder().setTimestamp(startTimeInMicroSec).addFields(Tag.newBuilder().setKey("event").setVStr("cs")).build());
            transactionTypeLogs.add(Log.newBuilder().setTimestamp(endTimeInMicroSec).addFields(Tag.newBuilder().setKey("event").setVStr("cr")).build());
        } else {
            throw new RuntimeException("TransactionType is missing from trace");
        }

        return transactionTypeLogs;
    }

    private List<Tag> getContextList(Map<String, String> kvs) {

        final List<Tag> contextList = new ArrayList<>();

        // if either of error, success or both the keys exist then create the error tag
        if (kvs.containsKey(ERROR_KEY)) {
            final boolean isError = "true".equalsIgnoreCase(kvs.get(ERROR_KEY));
            addErrorTag(contextList, isError);
        } else if (kvs.containsKey(SUCCESS_KEY)) {
            final boolean isError = "false".equalsIgnoreCase(kvs.get(SUCCESS_KEY));
            addErrorTag(contextList, isError);
        }
        for (Map.Entry<String, String> entry: kvs.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            if (key.equalsIgnoreCase(ERROR_KEY) || isTraceTag(key)) {
                continue;
            }

            final Tag.Builder tagBuilder = Tag.newBuilder().setType(Tag.TagType.STRING).setKey(key);
            if (value != null) {
                tagBuilder.setVStr(value);
            }
            contextList.add(tagBuilder.build());
        }

        return contextList;
    }

    private void addErrorTag(List<Tag> contextList, boolean isError) {
        final Tag.Builder errorTagBuilder = Tag.newBuilder().setType(Tag.TagType.BOOL).setKey(ERROR_KEY).setVBool(isError);
        contextList.add(errorTagBuilder.build());
    }

    private boolean isTraceTag(String key) {
        for (TraceTags t : TraceTags.values()) {
            if (t.getKey().equalsIgnoreCase(key))
                return true;
        }
        return false;
    }
}

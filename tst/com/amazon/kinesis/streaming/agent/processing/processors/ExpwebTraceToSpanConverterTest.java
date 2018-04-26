/*
 * Copyright (c) 2014-2017 Amazon.com, Inc. All Rights Reserved.
 */
package com.amazon.kinesis.streaming.agent.processing.processors;

import com.amazonaws.util.IOUtils;
import com.expedia.open.tracing.Span;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ExpwebTraceToSpanConverterTest {

  @Test
  public void testServerTransform() throws Exception {
    checkTransform(generateSpanForTraceRecord("expweb-server-trace.txt"), "3aca68e8-f128-461b-9adf-b5faaa6b0aac",
            "80cfc426-184a-48a6-8b45-d36f57142fb5", "",
            "Mozilla/5.0 (iPad; CPU OS 10_3_3 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Mobile/14G60");
  }

  @Test
  public void testClientTransform() throws Exception {
    checkTransform(generateSpanForTraceRecord("expweb-client-trace.txt"), "14761585-7e89-4a5e-b5db-117f69bbafaf",
            "78e33a8f-07c9-4b5c-8a61-31d552be9d8d", "81020bb4-c5e1-4b87-a5d6-fcca37d4cc75",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36");
  }


  @Test
  public void testTransformOfValuesWithQuote() throws Exception {
    checkTransform(generateSpanForTraceRecord("expweb-trace-quote.txt"), "f8fc580e-701d-4b06-b1b7-df61ed7d9af4", "26cfcee0-ee13-4099-8066-b0f1571d8e33", "",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36");
  }

  public void checkTransform(Span span, String expectedTraceId,
                             String expectedMessageId, String expectedParentMessageId, String expectedUA) throws Exception {
    final String parentSpanId = span.getParentSpanId();
    final String traceId = span.getTraceId();
    final String spanId = span.getSpanId();
    final String userAgent = span.getTagsList().stream().filter(tag -> tag.getKey().equalsIgnoreCase("useragent")).findFirst().get().getVStr();
    final String infraTagValue = span.getTagsList().stream().filter(tag -> tag.getKey().equals("X-HAYSTACK-INFRASTRUCTURE-PROVIDER")).findFirst().get().getVStr();
    Assert.assertEquals(infraTagValue, "dc");
    Assert.assertEquals(parentSpanId, expectedParentMessageId);
    Assert.assertEquals(traceId, expectedTraceId);
    Assert.assertEquals(spanId, expectedMessageId);
    Assert.assertEquals(userAgent, expectedUA);
  }

  private String loadTraceRecord(String fileName) throws IOException {
    return IOUtils.toString(this.getClass().getResourceAsStream(fileName));
  }

  private Span generateSpanForTraceRecord(final String traceRecordFileName) throws IOException {
    final String input = loadTraceRecord(traceRecordFileName);
    final ByteBuffer inputBuffer = ByteBuffer.wrap(input.getBytes());
    final byte[] output = new ExpwebTraceToSpanConverter().convert(inputBuffer).array();
    return Span.parseFrom(output);
  }

}

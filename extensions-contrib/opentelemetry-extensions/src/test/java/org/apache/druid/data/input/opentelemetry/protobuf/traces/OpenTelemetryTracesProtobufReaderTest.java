/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.opentelemetry.protobuf.traces;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.apache.commons.codec.binary.Hex;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.druid.data.input.opentelemetry.protobuf.TestUtils.assertDimensionEquals;

public class OpenTelemetryTracesProtobufReaderTest
{
  private static final String INSTRUMENTATION_SCOPE_NAME = "mock-instr-lib";
  private static final String INSTRUMENTATION_SCOPE_VERSION = "1.0";

  private static final String ATTRIBUTE_NAMESPACE = "namespace";
  private static final String ATTRIBUTE_VALUE_NAMESPACE = "namespace_val";
  private static final String ATTRIBUTE_SERVICE = "service";
  private static final String ATTRIBUTE_VALUE_TEST = "test-service";

  private static final String NAME_VALUE = "span-name";
  private static final byte[] SPAN_ID_VALUE = "abcd".getBytes(StandardCharsets.UTF_8);
  private static final String SPAN_ID_VALUE_HEX = Hex.encodeHexString(SPAN_ID_VALUE);
  private static final byte[] PARENT_ID_VALUE = "1234".getBytes(StandardCharsets.UTF_8);
  private static final String PARENT_ID_VALUE_HEX = Hex.encodeHexString(PARENT_ID_VALUE);
  private static final byte[] TRACE_ID_VALUE = "zyxwvutsrqponmlk".getBytes(StandardCharsets.UTF_8);
  private static final String TRACE_ID_VALUE_HEX = Hex.encodeHexString(TRACE_ID_VALUE);
  private static final Span.SpanKind SPAN_KIND_VALUE = Span.SpanKind.SPAN_KIND_SERVER;

  private final long now = System.nanoTime();
  private final long before = now - 1000;
  private final TracesData.Builder dataBuilder = TracesData.newBuilder();

  private final Span.Builder spanBuilder = dataBuilder
      .addResourceSpansBuilder()
      .addScopeSpansBuilder()
      .addSpansBuilder();

  private final OpenTelemetryTracesProtobufConfiguration config = OpenTelemetryTracesProtobufConfiguration
      .newBuilder()
      .setResourceAttributePrefix("resource.")
      .setSpanAttributePrefix("span.")
      .setKindDimension("kind")
      .setNameDimension("name")
      .setParentSpanIdDimension("parent_span_id")
      .setSpanIdDimension("span_id")
      .setTraceIdDimension("trace_id")
      .setStatusCodeDimension("status_code")
      .setStatusMessageDimension("status_message")
      .setEndTimeDimension("end_time")
      .build();

  private final DimensionsSpec dimensionsSpec = new DimensionsSpec(ImmutableList.of(
      new StringDimensionSchema("resource." + ATTRIBUTE_NAMESPACE),
      new StringDimensionSchema("span." + ATTRIBUTE_SERVICE)
  ));

  @Before
  public void setUp()
  {
    dataBuilder
        .getResourceSpansBuilder(0)
        .getResourceBuilder()
        .addAttributes(
            KeyValue.newBuilder()
                    .setKey(ATTRIBUTE_NAMESPACE)
                    .setValue(AnyValue.newBuilder().setStringValue(ATTRIBUTE_VALUE_NAMESPACE))
                    .build());

    dataBuilder
        .getResourceSpansBuilder(0)
        .getScopeSpansBuilder(0)
        .getScopeBuilder()
        .setName(INSTRUMENTATION_SCOPE_NAME)
        .setVersion(INSTRUMENTATION_SCOPE_VERSION);

    spanBuilder
        .setStartTimeUnixNano(before)
        .setEndTimeUnixNano(now)
        .setName(NAME_VALUE)
        .setStatus(Status.newBuilder().setCodeValue(1).setMessage("OK").build())
        .setTraceId(ByteString.copyFrom(TRACE_ID_VALUE))
        .setSpanId(ByteString.copyFrom(SPAN_ID_VALUE))
        .setParentSpanId(ByteString.copyFrom(PARENT_ID_VALUE))
        .setKind(SPAN_KIND_VALUE)
        .addAttributes(
            KeyValue.newBuilder()
                    .setKey(ATTRIBUTE_SERVICE)
                    .setValue(AnyValue.newBuilder().setStringValue(ATTRIBUTE_VALUE_TEST))
                    .build());
  }

  @Test
  public void testTrace()
  {
    CloseableIterator<InputRow> rows = getDataIterator(dimensionsSpec);
    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(2, row.getDimensions().size());
    verifyDefaultFirstRowData(row);
    assertDimensionEquals(row, "span.service", ATTRIBUTE_VALUE_TEST);
  }

  private CloseableIterator<InputRow> getDataIterator(DimensionsSpec spec)
  {
    TracesData tracesData = dataBuilder.build();
    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(tracesData.toByteArray()));
    return new OpenTelemetryTracesProtobufReader(
        spec,
        settableByteEntity,
        config
    ).read();
  }

  private void verifyDefaultFirstRowData(InputRow row)
  {
    assertDimensionEquals(row, "resource.namespace", ATTRIBUTE_VALUE_NAMESPACE);
    assertDimensionEquals(row, "name", NAME_VALUE);
    assertDimensionEquals(row, "span_id", SPAN_ID_VALUE_HEX);
    assertDimensionEquals(row, "parent_span_id", PARENT_ID_VALUE_HEX);
    assertDimensionEquals(row, "trace_id", TRACE_ID_VALUE_HEX);
    assertDimensionEquals(row, "end_time", Long.toString(TimeUnit.NANOSECONDS.toMillis(now)));
    assertDimensionEquals(row, "status_code", Integer.toString(1));
    assertDimensionEquals(row, "status_message", "OK");
    assertDimensionEquals(row, "kind", "SERVER");
  }
  @Test
  public void testBatchedTraceParse()
  {
    Span.Builder secondSpanBuilder = dataBuilder
        .addResourceSpansBuilder()
        .addScopeSpansBuilder()
        .addSpansBuilder();

    dataBuilder
        .getResourceSpansBuilder(1)
        .getResourceBuilder()
        .addAttributes(
            KeyValue.newBuilder()
                    .setKey(ATTRIBUTE_NAMESPACE)
                    .setValue(AnyValue.newBuilder().setStringValue(ATTRIBUTE_VALUE_NAMESPACE))
                    .build());
    dataBuilder
        .getResourceSpansBuilder(1)
        .getScopeSpansBuilder(0)
        .getScopeBuilder()
        .setName(INSTRUMENTATION_SCOPE_NAME)
        .setVersion(INSTRUMENTATION_SCOPE_VERSION);

    String name2 = "span-2";
    byte[] traceId2 = "trace-2".getBytes(StandardCharsets.UTF_8);
    byte[] spanId2 = "span-2".getBytes(StandardCharsets.UTF_8);
    byte[] parentId2 = "parent-2".getBytes(StandardCharsets.UTF_8);
    Span.SpanKind spanKind2 = Span.SpanKind.SPAN_KIND_CLIENT;
    String metricAttributeKey2 = "someIntAttribute";
    int metricAttributeVal2 = 23;
    String statusMessage2 = "NOT_OK";
    int statusCode2 = 400;

    secondSpanBuilder
        .setStartTimeUnixNano(before)
        .setEndTimeUnixNano(now)
        .setName(name2)
        .setStatus(Status.newBuilder().setCodeValue(statusCode2).setMessage(statusMessage2).build())
        .setTraceId(ByteString.copyFrom(traceId2))
        .setSpanId(ByteString.copyFrom(spanId2))
        .setParentSpanId(ByteString.copyFrom(parentId2))
        .setKind(spanKind2)
        .addAttributes(
            KeyValue.newBuilder()
                    .setKey(metricAttributeKey2)
                    .setValue(AnyValue.newBuilder().setIntValue(metricAttributeVal2))
                    .build());
    CloseableIterator<InputRow> rows = getDataIterator(dimensionsSpec);
    Assert.assertTrue(rows.hasNext());
    InputRow row = rows.next();
    Assert.assertEquals(2, row.getDimensions().size());
    assertDimensionEquals(row, "span.service", ATTRIBUTE_VALUE_TEST);
    verifyDefaultFirstRowData(row);

    Assert.assertTrue(rows.hasNext());
    row = rows.next();
    Assert.assertEquals(2, row.getDimensions().size());
    assertDimensionEquals(row, "resource.namespace", ATTRIBUTE_VALUE_NAMESPACE);
    assertDimensionEquals(row, "span.someIntAttribute", Integer.toString(metricAttributeVal2));
    assertDimensionEquals(row, "name", name2);
    assertDimensionEquals(row, "span_id", Hex.encodeHexString(spanId2));
    assertDimensionEquals(row, "parent_span_id", Hex.encodeHexString(parentId2));
    assertDimensionEquals(row, "trace_id", Hex.encodeHexString(traceId2));
    assertDimensionEquals(row, "end_time", Long.toString(TimeUnit.NANOSECONDS.toMillis(now)));
    assertDimensionEquals(row, "status_code", Integer.toString(statusCode2));
    assertDimensionEquals(row, "status_message", statusMessage2);
    assertDimensionEquals(row, "kind", "CLIENT");
  }

  @Test
  public void testDimensionSpecExclusions()
  {
    String excludedAttribute = "span." + ATTRIBUTE_SERVICE;
    DimensionsSpec dimensionsSpecWithExclusions = DimensionsSpec.builder().setDimensionExclusions(ImmutableList.of(
        excludedAttribute
    )).build();
    CloseableIterator<InputRow> rows = getDataIterator(dimensionsSpecWithExclusions);
    Assert.assertTrue(rows.hasNext());
    InputRow row = rows.next();
    Assert.assertFalse(row.getDimensions().contains(excludedAttribute));
    Assert.assertEquals(9, row.getDimensions().size());
    verifyDefaultFirstRowData(row);
  }

  @Test
  public void testInvalidProtobuf()
  {
    byte[] invalidProtobuf = new byte[] {0x04, 0x01};
    SettableByteEntity<ByteEntity> settableByteEntity = new SettableByteEntity<>();
    settableByteEntity.setEntity(new ByteEntity(invalidProtobuf));
    try (CloseableIterator<InputRow> rows = new OpenTelemetryTracesProtobufReader(
        dimensionsSpec,
        settableByteEntity,
        config
    ).read()) {
      Assert.assertThrows(ParseException.class, () -> rows.hasNext());
      Assert.assertThrows(ParseException.class, () -> rows.next());
    }
    catch (IOException e) {
      // Comes from the implicit call to close. Ignore
    }
  }
}

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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryProtobufExtensionsModule;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_END_TIME_DIMENSION;
import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_KIND_DIMENSION;
import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_PARENT_SPAN_ID_DIMENSION;
import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_RESOURCE_ATTR_PREFIX;
import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_SPAN_ATTR_PREFIX;
import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_SPAN_ID_DIMENSION;
import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_SPAN_NAME_DIMENSION;
import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_STATUS_CODE_DIMENSION;
import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_STATUS_MESSAGE_DIMENSION;
import static org.apache.druid.data.input.opentelemetry.protobuf.traces.OpenTelemetryTracesProtobufInputFormat.DEFAULT_TRACE_ID_DIMENSION;
import static org.junit.Assert.assertEquals;

public class OpenTelemetryTracesProtobufInputFormatTest
{
  final ObjectMapper jsonMapper = new ObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    String resourceAttrPrefix = "test.resource.";
    String spanAttrPrefix = "test.span.";
    String kind = "test.kind";
    String spanName = "test.name";
    String parentSpanId = "test.parent.span";
    String spanId = "test.span_id";
    String traceId = "test.trace.id";
    String statusCode = "test.status.code";
    String statusMessage = "test.status.message";
    String endTime = "test.end.time";
    OpenTelemetryTracesProtobufInputFormat inputFormat = new OpenTelemetryTracesProtobufInputFormat(
        "test.span.",
        "test.resource.",
        "test.name",
        "test.span_id",
        "test.parent.span",
        "test.trace.id",
        "test.end.time",
        "test.status.code",
        "test.status.message",
        "test.kind");
    jsonMapper.registerModules(new OpenTelemetryProtobufExtensionsModule().getJacksonModules());

    final OpenTelemetryTracesProtobufInputFormat actual = (OpenTelemetryTracesProtobufInputFormat) jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        InputFormat.class
    );

    Assert.assertEquals(inputFormat, actual);
  }

  @Test
  public void testDefaults() throws Exception
  {
    verifyDefaultFields(new OpenTelemetryTracesProtobufInputFormat(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null));
    verifyDefaultFields(jsonMapper.readValue(
        "{}",
        OpenTelemetryTracesProtobufInputFormat.class
    ));
  }

  private void verifyDefaultFields(OpenTelemetryTracesProtobufInputFormat obj)
  {
    assertEquals(DEFAULT_RESOURCE_ATTR_PREFIX, obj.getResourceAttributePrefix());
    assertEquals(DEFAULT_SPAN_ATTR_PREFIX, obj.getSpanAttributePrefix());
    assertEquals(DEFAULT_KIND_DIMENSION, obj.getKindDimension());
    assertEquals(DEFAULT_SPAN_NAME_DIMENSION, obj.getSpanNameDimension());
    assertEquals(DEFAULT_PARENT_SPAN_ID_DIMENSION, obj.getParentSpanIdDimension());
    assertEquals(DEFAULT_SPAN_ID_DIMENSION, obj.getSpanIdDimension());
    assertEquals(DEFAULT_TRACE_ID_DIMENSION, obj.getTraceIdDimension());
    assertEquals(DEFAULT_STATUS_MESSAGE_DIMENSION, obj.getStatusMessageDimension());
    assertEquals(DEFAULT_STATUS_CODE_DIMENSION, obj.getStatusCodeDimension());
    assertEquals(DEFAULT_END_TIME_DIMENSION, obj.getEndTimeDimension());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(OpenTelemetryTracesProtobufInputFormat.class).usingGetClass().verify();
  }
}

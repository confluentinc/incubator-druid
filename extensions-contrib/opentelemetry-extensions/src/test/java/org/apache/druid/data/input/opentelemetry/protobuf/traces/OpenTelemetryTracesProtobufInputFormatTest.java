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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryProtobufExtensionsModule;
import org.junit.Assert;
import org.junit.Test;

public class OpenTelemetryTracesProtobufInputFormatTest
{

  @Test
  public void testSerde() throws Exception
  {
    String resourceAttrPrefix = "test.resource";
    String spanAttrPrefix = "test.span.";
    String kind = "test.kind";
    String name = "test.name";
    String parentSpanId = "test.parent.span";
    String spanId = "test.span.id";
    String traceId = "test.trace.id";
    String statusCode = "test.status.code";
    String statusMessage = "test.status.message";
    String endTime = "test.end.time";
    OpenTelemetryTracesProtobufConfiguration config = OpenTelemetryTracesProtobufConfiguration
        .newBuilder()
        .setResourceAttributePrefix(resourceAttrPrefix)
        .setSpanAttributePrefix(spanAttrPrefix)
        .setKindDimension(kind)
        .setNameDimension(name)
        .setParentSpanIdDimension(parentSpanId)
        .setSpanIdDimension(spanId)
        .setTraceIdDimension(traceId)
        .setStatusCodeDimension(statusCode)
        .setStatusMessageDimension(statusMessage)
        .setEndTimeDimension(endTime)
        .build();
    OpenTelemetryTracesProtobufInputFormat inputFormat = new OpenTelemetryTracesProtobufInputFormat(config);

    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.registerModules(new OpenTelemetryProtobufExtensionsModule().getJacksonModules());

    final OpenTelemetryTracesProtobufInputFormat actual = (OpenTelemetryTracesProtobufInputFormat) jsonMapper.readValue(
        jsonMapper.writeValueAsString(inputFormat),
        InputFormat.class
    );

    Assert.assertEquals(inputFormat, actual);
  }
}

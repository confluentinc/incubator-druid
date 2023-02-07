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
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryProtobufExtensionsModule;
import org.junit.Assert;
import org.junit.Test;

public class OpenTelemetryTracesProtobufConfigurationTest
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

    final ObjectMapper jsonMapper = new ObjectMapper();
    jsonMapper.registerModules(new OpenTelemetryProtobufExtensionsModule().getJacksonModules());

    final OpenTelemetryTracesProtobufConfiguration actual = (OpenTelemetryTracesProtobufConfiguration) jsonMapper.readValue(
        jsonMapper.writeValueAsString(config),
        OpenTelemetryTracesProtobufConfiguration.class
    );
    Assert.assertEquals(config, actual);
    Assert.assertEquals(resourceAttrPrefix, actual.getResourceAttributePrefix());
    Assert.assertEquals(spanAttrPrefix, actual.getSpanAttributePrefix());
    Assert.assertEquals(kind, actual.getKindDimension());
    Assert.assertEquals(name, actual.getNameDimension());
    Assert.assertEquals(parentSpanId, actual.getParentSpanIdDimension());
    Assert.assertEquals(spanId, actual.getSpanIdDimension());
    Assert.assertEquals(traceId, actual.getTraceIdDimension());
    Assert.assertEquals(statusMessage, actual.getStatusMessageDimension());
    Assert.assertEquals(statusCode, actual.getStatusCodeDimension());
    Assert.assertEquals(endTime, actual.getEndTimeDimension());
  }

  @Test
  public void testDefaults() throws Exception
  {
    OpenTelemetryTracesProtobufConfiguration config = OpenTelemetryTracesProtobufConfiguration
        .newBuilder()
        .build();
    Assert.assertEquals("resource.", config.getResourceAttributePrefix());
    Assert.assertEquals("", config.getSpanAttributePrefix());
    Assert.assertEquals("kind", config.getKindDimension());
    Assert.assertEquals("name", config.getNameDimension());
    Assert.assertEquals("parent.span.id", config.getParentSpanIdDimension());
    Assert.assertEquals("span.id", config.getSpanIdDimension());
    Assert.assertEquals("trace.id", config.getTraceIdDimension());
    Assert.assertEquals("status.message", config.getStatusMessageDimension());
    Assert.assertEquals("status.code", config.getStatusCodeDimension());
    Assert.assertEquals("end.time", config.getEndTimeDimension());
  }
}

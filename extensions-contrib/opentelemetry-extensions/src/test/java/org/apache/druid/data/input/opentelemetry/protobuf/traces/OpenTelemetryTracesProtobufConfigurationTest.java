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
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryProtobufExtensionsModule;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OpenTelemetryTracesProtobufConfigurationTest
{

  final ObjectMapper jsonMapper = new ObjectMapper();

  @Before
  public void setup()
  {
    jsonMapper.registerModules(new OpenTelemetryProtobufExtensionsModule().getJacksonModules());
  }

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

    final OpenTelemetryTracesProtobufConfiguration actual = (OpenTelemetryTracesProtobufConfiguration) jsonMapper
        .readValue(
          jsonMapper.writeValueAsString(config),
          OpenTelemetryTracesProtobufConfiguration.class
        );
    assertEquals(config, actual);
    assertEquals(resourceAttrPrefix, actual.getResourceAttributePrefix());
    assertEquals(spanAttrPrefix, actual.getSpanAttributePrefix());
    assertEquals(kind, actual.getKindDimension());
    assertEquals(name, actual.getNameDimension());
    assertEquals(parentSpanId, actual.getParentSpanIdDimension());
    assertEquals(spanId, actual.getSpanIdDimension());
    assertEquals(traceId, actual.getTraceIdDimension());
    assertEquals(statusMessage, actual.getStatusMessageDimension());
    assertEquals(statusCode, actual.getStatusCodeDimension());
    assertEquals(endTime, actual.getEndTimeDimension());
  }

  @Test
  public void testDefaults() throws Exception
  {
    verifyDefaultFields(OpenTelemetryTracesProtobufConfiguration
                            .newBuilder()
                            .build());
    verifyDefaultFields(jsonMapper.readValue(
        "{}",
        OpenTelemetryTracesProtobufConfiguration.class
    ));
  }

  private void verifyDefaultFields(OpenTelemetryTracesProtobufConfiguration config)
  {
    assertEquals("resource.", config.getResourceAttributePrefix());
    assertEquals("span.", config.getSpanAttributePrefix());
    assertEquals("kind", config.getKindDimension());
    assertEquals("name", config.getNameDimension());
    assertEquals("parent.span.id", config.getParentSpanIdDimension());
    assertEquals("span.id", config.getSpanIdDimension());
    assertEquals("trace.id", config.getTraceIdDimension());
    assertEquals("status.message", config.getStatusMessageDimension());
    assertEquals("status.code", config.getStatusCodeDimension());
    assertEquals("end.time", config.getEndTimeDimension());
  }

  @Test
  public void testEquals() throws Exception
  {
    EqualsVerifier.forClass(OpenTelemetryTracesProtobufConfiguration.class).usingGetClass().verify();
  }
}

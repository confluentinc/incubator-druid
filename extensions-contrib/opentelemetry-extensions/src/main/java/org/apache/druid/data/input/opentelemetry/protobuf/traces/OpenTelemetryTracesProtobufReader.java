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

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.TracesData;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryProtobufReader;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OpenTelemetryTracesProtobufReader extends OpenTelemetryProtobufReader
{
  private final OpenTelemetryTracesProtobufConfiguration config;

  public OpenTelemetryTracesProtobufReader(
      DimensionsSpec dimensionsSpec,
      SettableByteEntity<? extends ByteEntity> source,
      OpenTelemetryTracesProtobufConfiguration config
  )
  {
    super(dimensionsSpec, source, config.getResourceAttributePrefix());
    this.config = config;
  }

  @Override
  public List<InputRow> parseData(ByteBuffer byteBuffer)
      throws InvalidProtocolBufferException
  {
    return parseTracesData(TracesData.parseFrom(byteBuffer));
  }

  private List<InputRow> parseTracesData(final TracesData tracesData)
  {
    return tracesData.getResourceSpansList()
        .stream()
        .flatMap(resourceSpans -> {
          Map<String, Object> resourceAttributes = getResourceAttributes(resourceSpans.getResource());
          return resourceSpans.getScopeSpansList()
              .stream()
              .flatMap(scopeSpans -> scopeSpans.getSpansList()
                   .stream()
                   .map(span -> parseSpan(span, resourceAttributes)));
        })
        .collect(Collectors.toList());
  }

  private InputRow parseSpan(Span span, Map<String, Object> resourceAttributes)
  {
    int capacity = resourceAttributes.size() + span.getAttributesCount() +
                   OpenTelemetryTracesProtobufConfiguration.DEFAULT_COLUMN_COUNT;
    Map<String, Object> event = Maps.newHashMapWithExpectedSize(capacity);
    event.put(config.getNameDimension(), span.getName());
    event.put(config.getSpanIdDimension(), Hex.encodeHexString(span.getSpanId().asReadOnlyByteBuffer()));
    event.put(config.getParentSpanIdDimension(), Hex.encodeHexString(span.getParentSpanId().asReadOnlyByteBuffer()));
    event.put(config.getTraceIdDimension(), Hex.encodeHexString(span.getTraceId().asReadOnlyByteBuffer()));
    event.put(config.getEndTimeDimension(), TimeUnit.NANOSECONDS.toMillis(span.getEndTimeUnixNano()));
    event.put(config.getStatusCodeDimension(), span.getStatus().getCodeValue());
    event.put(config.getStatusMessageDimension(), span.getStatus().getMessage());
    event.put(config.getKindDimension(), StringUtils.replace(span.getKind().toString(), "SPAN_KIND_", ""));
    event.putAll(resourceAttributes);
    span.getAttributesList().forEach(att -> {
      Object value = parseAnyValue(att.getValue());
      if (value != null) {
        event.put(config.getSpanAttributePrefix() + att.getKey(), value);
      }
    });

    return createRow(TimeUnit.NANOSECONDS.toMillis(span.getStartTimeUnixNano()), event);
  }
}

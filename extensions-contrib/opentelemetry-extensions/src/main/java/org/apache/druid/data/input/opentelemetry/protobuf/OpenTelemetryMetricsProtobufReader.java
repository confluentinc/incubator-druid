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

package org.apache.druid.data.input.opentelemetry.protobuf;

import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.IntDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class OpenTelemetryMetricsProtobufReader implements InputEntityReader
{

  private final ByteEntity source;
  private final String metricDimension;
  private final String valueDimension;
  private final String metricAttributePrefix;
  private final String resourceAttributePrefix;
  private final Predicate<String> attributeMembership;
  private final List<String> schemaDimensions;

  public OpenTelemetryMetricsProtobufReader(
      DimensionsSpec dimensionsSpec,
      ByteEntity source,
      String metricDimension,
      String valueDimension,
      String metricLabelPrefix,
      String resourceLabelPrefix
  )
  {
    this.source = source;
    this.metricDimension = metricDimension;
    this.valueDimension = valueDimension;
    this.metricAttributePrefix = metricLabelPrefix;
    this.resourceAttributePrefix = resourceLabelPrefix;

    this.schemaDimensions = dimensionsSpec.getDimensionNames();
    if (!schemaDimensions.isEmpty()) {
      this.attributeMembership = schemaDimensions::contains;
    } else {
      this.attributeMembership = att -> !dimensionsSpec.getDimensionExclusions().contains(att);
    }
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    return CloseableIterators.withEmptyBaggage(readAsList().iterator());
  }

  List<InputRow> readAsList()
  {
    try {
      return parseMetricsData(MetricsData.parseFrom(source.getBuffer()));
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(e, "Protobuf message could not be parsed");
    }
  }

  private List<InputRow> parseMetricsData(final MetricsData metricsData)
  {
    return metricsData.getResourceMetricsList()
            .stream()
            .flatMap(resourceMetrics -> resourceMetrics.getInstrumentationLibraryMetricsList()
                    .stream()
                    .flatMap(libraryMetrics -> libraryMetrics.getMetricsList()
                            .stream()
                            .flatMap(metric -> parseMetric(metric, resourceMetrics.getResource().getAttributesList())
                                    .stream())))
            .collect(Collectors.toList());
  }

  private List<InputRow> parseMetric(Metric metric, List<KeyValue> resourceAttributes)
  {
    switch (metric.getDataCase()) {
      case INT_SUM: {
        return metric.getIntSum()
                .getDataPointsList()
                .stream()
                .map(OpenTelemetryMetricsProtobufReader::intDataPointToNumberDataPoint)
                .map(dataPoint -> parseNumberDataPoint(dataPoint, metric.getName(), resourceAttributes))
                .collect(Collectors.toList());
      }
      case INT_GAUGE: {
        return metric.getIntGauge()
                .getDataPointsList()
                .stream()
                .map(OpenTelemetryMetricsProtobufReader::intDataPointToNumberDataPoint)
                .map(dataPoint -> parseNumberDataPoint(dataPoint, metric.getName(), resourceAttributes))
                .collect(Collectors.toList());
      }
      case SUM: {
        return metric.getSum()
                .getDataPointsList()
                .stream()
                .map(dataPoint -> parseNumberDataPoint(dataPoint, metric.getName(), resourceAttributes))
                .collect(Collectors.toList());
      }
      case GAUGE: {
        return metric.getGauge()
                .getDataPointsList()
                .stream()
                .map(dataPoint -> parseNumberDataPoint(dataPoint, metric.getName(), resourceAttributes))
                .collect(Collectors.toList());
      }
      // TODO Support HISTOGRAM and SUMMARY metrics
      default:
        throw new IllegalStateException("Unexpected value: " + metric.getDataCase());
    }
  }

  private static NumberDataPoint intDataPointToNumberDataPoint(IntDataPoint dataPoint)
  {
    return NumberDataPoint.newBuilder()
            .setTimeUnixNano(dataPoint.getTimeUnixNano())
            .setAsInt(dataPoint.getValue())
            .addAllAttributes(dataPoint.getLabelsList()
                    .stream()
                    .map(label -> KeyValue.newBuilder()
                            .setKey(label.getKey())
                            .setValue(AnyValue.newBuilder().setStringValue(label.getValue()))
                            .build())
                    .collect(Collectors.toList()))
            .build();
  }

  private InputRow parseNumberDataPoint(NumberDataPoint dataPoint,
                                        String metricName,
                                        List<KeyValue> resourceAttributes)
  {
    long timeUnixMilli = TimeUnit.NANOSECONDS.toMillis(dataPoint.getTimeUnixNano());
    List<KeyValue> metricAttributes = labelsToAttributes(dataPoint);
    Number value = getValue(dataPoint);

    int capacity = resourceAttributes.size()
            + metricAttributes.size()
            + 2; // metric name + value columns

    Map<String, Object> event = Maps.newHashMapWithExpectedSize(capacity);
    event.put(this.metricDimension, metricName);
    event.put(this.valueDimension, value);

    resourceAttributes.stream()
            .filter(ra -> this.attributeMembership.test(this.resourceAttributePrefix + ra.getKey()))
            .forEach(ra -> event.put(this.resourceAttributePrefix + ra.getKey(), ra.getValue().getStringValue()));

    metricAttributes.stream()
            .filter(ma -> this.attributeMembership.test(this.metricAttributePrefix + ma.getKey()))
            .forEach(ma -> event.put(this.metricAttributePrefix + ma.getKey(), ma.getValue().getStringValue()));

    List<String> dimensions = this.schemaDimensions.isEmpty() ? new ArrayList<>(event.keySet()) : this.schemaDimensions;
    return new MapBasedInputRow(timeUnixMilli, dimensions, event);
  }

  private static List<KeyValue> labelsToAttributes(NumberDataPoint dataPoint)
  {
    if (!dataPoint.getAttributesList().isEmpty()) {
      return dataPoint.getAttributesList();
    }
    return dataPoint.getLabelsList()
            .stream()
            .map(label -> KeyValue.newBuilder()
                    .setKey(label.getKey())
                    .setValue(AnyValue.newBuilder().setStringValue(label.getValue()))
                    .build())
            .collect(Collectors.toList());
  }

  private static Number getValue(NumberDataPoint dataPoint)
  {
    if (dataPoint.hasAsInt()) {
      return dataPoint.getAsInt();
    } else if (dataPoint.hasAsDouble()) {
      return dataPoint.getAsDouble();
    } else {
      throw new IllegalStateException("Unexpected dataPoint value type. Expected Int or Double");
    }
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample()
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}

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
  private static final String VALUE_COLUMN = "value";

  private final ByteEntity source;
  private final String metricDimension;
  private final String metricAttributePrefix;
  private final String resourceAttributePrefix;
  private final Predicate<String> attributeMembership;

  public OpenTelemetryMetricsProtobufReader(
      DimensionsSpec dimensionsSpec,
      ByteEntity source,
      String metricDimension,
      String metricLabelPrefix,
      String resourceLabelPrefix
  )
  {
    this.source = source;
    this.metricDimension = metricDimension;
    this.metricAttributePrefix = metricLabelPrefix;
    this.resourceAttributePrefix = resourceLabelPrefix;

    List<String> schemaDimensions = dimensionsSpec.getDimensionNames();
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
                                    .stream()
                            )
                    )
            ).collect(Collectors.toList());
  }

  private List<InputRow> parseMetric(Metric metric, List<KeyValue> resourceAttributes)
  {
    switch (metric.getDataCase()) {
      case INT_SUM: {
        List<NumberDataPoint> dataPoints = metric.getIntSum()
                .getDataPointsList()
                .stream()
                .map(OpenTelemetryMetricsProtobufReader::intDataPointToNumDataPoint)
                .collect(Collectors.toList());
        return parseNumDataPoints(dataPoints, metric.getName(), resourceAttributes);
      }
      case INT_GAUGE: {
        List<NumberDataPoint> dataPoints = metric.getIntGauge()
                .getDataPointsList()
                .stream()
                .map(OpenTelemetryMetricsProtobufReader::intDataPointToNumDataPoint)
                .collect(Collectors.toList());
        return parseNumDataPoints(dataPoints, metric.getName(), resourceAttributes);
      }
      case SUM: {
        return parseNumDataPoints(metric.getSum().getDataPointsList(), metric.getName(), resourceAttributes);
      }
      case GAUGE: {
        return parseNumDataPoints(metric.getGauge().getDataPointsList(), metric.getName(), resourceAttributes);
      }
      // TODO Support HISTOGRAM and SUMMARY
      default:
        throw new IllegalStateException("Unexpected value: " + metric.getDataCase());
    }
  }

  private List<InputRow> parseNumDataPoints(List<NumberDataPoint> dataPoints,
                                            String metricName,
                                            List<KeyValue> resourceAttributes)
  {
    List<InputRow> rows = new ArrayList<>();
    for (NumberDataPoint dataPoint : dataPoints) {

      long timeUnixMilli = TimeUnit.NANOSECONDS.toMillis(dataPoint.getTimeUnixNano());
      List<KeyValue> metricAttributes = dataPoint.getAttributesList();
      Number value = getValue(dataPoint);

      int capacity = resourceAttributes.size()
              + metricAttributes.size()
              + 2; // metric name + value columns

      Map<String, Object> event = Maps.newHashMapWithExpectedSize(capacity);
      event.put(metricDimension, metricName);
      event.put(VALUE_COLUMN, value);

      resourceAttributes.stream()
              .filter(ra -> attributeMembership.test(this.resourceAttributePrefix + ra.getKey()))
              .forEach(ra -> event.put(this.resourceAttributePrefix + ra.getKey(), ra.getValue().getStringValue()));

      metricAttributes.stream()
              .filter(ma -> attributeMembership.test(this.metricAttributePrefix + ma.getKey()))
              .forEach(ma -> event.put(this.metricAttributePrefix + ma.getKey(), ma.getValue().getStringValue()));

      rows.add(new MapBasedInputRow(timeUnixMilli, new ArrayList<>(event.keySet()), event));
    }
    return rows;
  }

  private static NumberDataPoint intDataPointToNumDataPoint(IntDataPoint dataPoint)
  {
    NumberDataPoint.Builder builder = NumberDataPoint.newBuilder()
            .setTimeUnixNano(dataPoint.getTimeUnixNano())
            .setAsInt(dataPoint.getValue());

    dataPoint.getLabelsList().forEach(label -> builder.addAttributes(
            KeyValue.newBuilder()
                    .setKey(label.getKey())
                    .setValue(AnyValue.newBuilder().setStringValue(label.getValue()))
            ));
    return builder.build();
  }

  private static Number getValue(NumberDataPoint dataPoint)
  {
    if (dataPoint.hasAsInt()) {
      return dataPoint.getAsInt();
    } else {
      return dataPoint.getAsDouble();
    }
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample()
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}

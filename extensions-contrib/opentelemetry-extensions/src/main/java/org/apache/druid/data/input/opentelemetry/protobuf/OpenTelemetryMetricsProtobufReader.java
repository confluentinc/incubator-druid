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
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
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
import java.util.function.Function;
import java.util.stream.Collectors;

public class OpenTelemetryMetricsProtobufReader implements InputEntityReader
{
  private static final String VALUE_COLUMN = "value";

  private final DimensionsSpec dimensionsSpec;
  private final ByteEntity source;
  private final String metricDimension;
  private final String metricAttributePrefix;
  private final String resourceAttributePrefix;

  public OpenTelemetryMetricsProtobufReader(
      DimensionsSpec dimensionsSpec,
      ByteEntity source,
      String metricDimension,
      String metricLabelPrefix,
      String resourceLabelPrefix
  )
  {
    this.dimensionsSpec = dimensionsSpec;
    this.source = source;
    this.metricDimension = metricDimension;
    this.metricAttributePrefix = metricLabelPrefix;
    this.resourceAttributePrefix = resourceLabelPrefix;
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    return CloseableIterators.withEmptyBaggage(readAsList().iterator());
  }

  List<InputRow> readAsList()
  {
    try {
      return parseRequest(MetricsData.parseFrom(source.getBuffer()));
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(e, "Protobuf message could not be parsed");
    }
  }

  private List<InputRow> parseRequest(final MetricsData request)
  {

    List<ResourceMetrics> resourceMetricsList = request.getResourceMetricsList();
    return resourceMetricsList.stream()
            .flatMap(
            resourceMetrics -> {
              List<KeyValue> resourceAttributes = resourceMetrics.getResource().getAttributesList();
              return resourceMetrics.getInstrumentationLibraryMetricsList()
                      .stream()
                      .flatMap(libraryMetrics -> libraryMetrics.getMetricsList()
                              .stream()
                              .map(metric -> parseMetric(metric, resourceAttributes))
                              .flatMap(List::stream)
                      );
            }
    ).collect(Collectors.toList());
  }

  private List<InputRow> parseMetric(Metric metric, List<KeyValue> resourceAttributes)
  {

    List<String> schemaDimensions = dimensionsSpec.getDimensionNames();
    if (!schemaDimensions.isEmpty()) {
      return createRows(metric, resourceAttributes, schemaDimensions::contains);
    } else {
      return createRows(metric, resourceAttributes, att -> !dimensionsSpec.getDimensionExclusions().contains(att));
    }
  }

  private List<InputRow> createRows(Metric metric,
                                    List<KeyValue> resourceAttributes,
                                    Function<String, Boolean> attributeMembershipFn)
  {

    List<NumberDataPoint> dataPoints = getDataPoints(metric);
    List<InputRow> rows = new ArrayList<>();

    for (NumberDataPoint dataPoint : dataPoints) {

      final long timeUnixMilli = TimeUnit.NANOSECONDS.toMillis(dataPoint.getTimeUnixNano());
      final List<KeyValue> metricAttributes = dataPoint.getAttributesList();
      final Number value = getValue(dataPoint);

      final int capacity = resourceAttributes.size()
              + metricAttributes.size()
              + 2; // metric name + value columns

      Map<String, Object> event = Maps.newHashMapWithExpectedSize(capacity);
      event.put(metricDimension, metric.getName());
      event.put(VALUE_COLUMN, value);

      List<String> dimensions = new ArrayList<>();
      dimensions.add(metricDimension);
      dimensions.add(VALUE_COLUMN);

      resourceAttributes.stream()
              .filter(ra -> attributeMembershipFn.apply(this.resourceAttributePrefix + ra.getKey()))
              .forEach(ra -> {
                dimensions.add(this.resourceAttributePrefix + ra.getKey());
                event.put(this.resourceAttributePrefix + ra.getKey(), ra.getValue().getStringValue());
              });

      metricAttributes.stream()
              .filter(ma -> attributeMembershipFn.apply(this.metricAttributePrefix + ma.getKey()))
              .forEach(ma -> {
                dimensions.add(this.metricAttributePrefix + ma.getKey());
                event.put(this.metricAttributePrefix + ma.getKey(), ma.getValue().getStringValue());
              });

      rows.add(new MapBasedInputRow(timeUnixMilli, dimensions, event));
    }
    return rows;
  }

  private List<NumberDataPoint> getDataPoints(Metric metric)
  {
    List<NumberDataPoint> dataPoints;
    switch (metric.getDataCase()) {
      case INT_SUM: {
        dataPoints = metric.getIntSum()
                .getDataPointsList()
                .stream()
                .map(this::dataPointConverter)
                .collect(Collectors.toList());
        break;
      }
      case INT_GAUGE: {
        dataPoints = metric.getIntGauge()
                .getDataPointsList()
                .stream()
                .map(this::dataPointConverter)
                .collect(Collectors.toList());
        break;
      }
      case SUM: {
        dataPoints = metric.getSum().getDataPointsList();
        break;
      }
      case GAUGE: {
        dataPoints = metric.getGauge().getDataPointsList();
        break;
      }
      default:
        throw new IllegalStateException("Unexpected value: " + metric.getDataCase());
    }
    return dataPoints;
  }

  private NumberDataPoint dataPointConverter(IntDataPoint dataPoint)
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

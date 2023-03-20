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

package org.apache.druid.data.input.opencensus.protobuf;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import io.opencensus.proto.metrics.v1.LabelKey;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.TimeSeries;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.opentelemetry.protobuf.AbstractProtobufReader;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OpenCensusProtobufReader extends AbstractProtobufReader
{
  private static final String SEPARATOR = "-";
  private static final String VALUE_COLUMN = "value";

  private final String metricDimension;
  private final String metricLabelPrefix;
  private final String resourceLabelPrefix;

  public OpenCensusProtobufReader(
      DimensionsSpec dimensionsSpec,
      SettableByteEntity<? extends ByteEntity> source,
      String metricDimension,
      String metricLabelPrefix,
      String resourceLabelPrefix
  )
  {
    super(dimensionsSpec, source);
    this.metricDimension = metricDimension;
    this.metricLabelPrefix = metricLabelPrefix;
    this.resourceLabelPrefix = resourceLabelPrefix;
  }

  private interface LabelContext
  {
    void addRow(long millis, String metricName, Object value);
  }

  @Override
  public List<InputRow> parseData(ByteBuffer byteBuffer) throws InvalidProtocolBufferException
  {
    return parseMetric(Metric.parseFrom(byteBuffer));
  }

  private List<InputRow> parseMetric(final Metric metric)
  {
    // Process metric descriptor labels map keys.
    List<String> descriptorLabels = new ArrayList<>(metric.getMetricDescriptor().getLabelKeysCount());
    for (LabelKey s : metric.getMetricDescriptor().getLabelKeysList()) {
      descriptorLabels.add(this.metricLabelPrefix + s.getKey());
    }

    // Process resource labels map.
    Map<String, String> resourceLabelsMap = CollectionUtils.mapKeys(
        metric.getResource().getLabelsMap(),
        key -> this.resourceLabelPrefix + key
    );

    final List<String> schemaDimensions = dimensionsSpec.getDimensionNames();

    final List<String> dimensions;
    if (!schemaDimensions.isEmpty()) {
      dimensions = schemaDimensions;
    } else {
      Set<String> recordDimensions = new HashSet<>(descriptorLabels);

      // Add resource map key set to record dimensions.
      recordDimensions.addAll(resourceLabelsMap.keySet());

      // MetricDimension, VALUE dimensions will not be present in labelKeysList or Metric.Resource
      // map as they are derived dimensions, which get populated while parsing data for timeSeries
      // hence add them to recordDimensions.
      recordDimensions.add(metricDimension);
      recordDimensions.add(VALUE_COLUMN);

      dimensions = Lists.newArrayList(
          Sets.difference(recordDimensions, dimensionsSpec.getDimensionExclusions())
      );
    }

    final int capacity = resourceLabelsMap.size()
                         + descriptorLabels.size()
                         + 2; // metric name + value columns

    List<InputRow> rows = new ArrayList<>();
    for (TimeSeries ts : metric.getTimeseriesList()) {
      final LabelContext labelContext = (millis, metricName, value) -> {
        // Add common resourceLabels.
        Map<String, Object> event = Maps.newHashMapWithExpectedSize(capacity);
        event.putAll(resourceLabelsMap);
        // Add metric labels
        for (int i = 0; i < metric.getMetricDescriptor().getLabelKeysCount(); i++) {
          event.put(descriptorLabels.get(i), ts.getLabelValues(i).getValue());
        }
        // add metric name and value
        event.put(metricDimension, metricName);
        event.put(VALUE_COLUMN, value);
        rows.add(new MapBasedInputRow(millis, dimensions, event));
      };

      for (Point point : ts.getPointsList()) {
        addPointRows(point, metric, labelContext);
      }
    }
    return rows;
  }

  private void addPointRows(Point point, Metric metric, LabelContext labelContext)
  {
    Timestamp timestamp = point.getTimestamp();
    long millis = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli();
    String metricName = metric.getMetricDescriptor().getName();

    switch (point.getValueCase()) {
      case DOUBLE_VALUE:
        labelContext.addRow(millis, metricName, point.getDoubleValue());
        break;

      case INT64_VALUE:
        labelContext.addRow(millis, metricName, point.getInt64Value());
        break;

      case SUMMARY_VALUE:
        // count
        labelContext.addRow(
            millis,
            metricName + SEPARATOR + "count",
            point.getSummaryValue().getCount().getValue()
        );
        // sum
        labelContext.addRow(
            millis,
            metricName + SEPARATOR + "sum",
            point.getSummaryValue().getSnapshot().getSum().getValue()
        );
        break;

      // TODO : How to handle buckets and percentiles
      case DISTRIBUTION_VALUE:
        // count
        labelContext.addRow(millis, metricName + SEPARATOR + "count", point.getDistributionValue().getCount());
        // sum
        labelContext.addRow(
            millis,
            metricName + SEPARATOR + "sum",
            point.getDistributionValue().getSum()
        );
        break;
      default:
    }
  }
}

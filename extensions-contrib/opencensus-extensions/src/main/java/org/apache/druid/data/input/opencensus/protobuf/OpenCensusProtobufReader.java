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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.Timestamp;
import io.opencensus.proto.metrics.v1.LabelKey;
import io.opencensus.proto.metrics.v1.Metric;
import io.opencensus.proto.metrics.v1.Point;
import io.opencensus.proto.metrics.v1.TimeSeries;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.KafkaUtils;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryMetricsProtobufReader;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.utils.CollectionUtils;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.druid.data.input.opencensus.protobuf.OpenCensusProtobufReader.ProtobufReader.OPEN_CENSUS;
import static org.apache.druid.data.input.opencensus.protobuf.OpenCensusProtobufReader.ProtobufReader.OPEN_TELEMETRY;

public class OpenCensusProtobufReader implements InputEntityReader
{
  private static final String SEPARATOR = "-";
  private static final String VALUE_COLUMN = "value";
  private static final String VERSION_HEADER_KEY = "v";
  private static final int OPENTELEMETRY_FORMAT_VERSION = 1;

  private final DimensionsSpec dimensionsSpec;
  private final SettableByteEntity<? extends ByteEntity> source;
  private final String metricDimension;
  private final String valueDimension;
  private final String metricLabelPrefix;
  private final String resourceLabelPrefix;

  enum ProtobufReader {
    OPEN_CENSUS,
    OPEN_TELEMETRY
  }

  InputEntityReader openCensusReader = newReader(OPEN_CENSUS);
  InputEntityReader openTelemetryReader = newReader(OPEN_TELEMETRY);

  public OpenCensusProtobufReader(
      DimensionsSpec dimensionsSpec,
      SettableByteEntity<? extends ByteEntity> source,
      String metricDimension,
      String valueDimension,
      String metricLabelPrefix,
      String resourceLabelPrefix
  )
  {
    this.dimensionsSpec = dimensionsSpec;
    this.source = source;
    this.metricDimension = metricDimension;
    this.valueDimension = valueDimension;
    this.metricLabelPrefix = metricLabelPrefix;
    this.resourceLabelPrefix = resourceLabelPrefix;
  }

  private interface LabelContext
  {
    void addRow(long millis, String metricName, Object value);
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return whichReader() == OPEN_TELEMETRY ? openTelemetryReader.read() : openCensusReader.read();
  }

  class OpenCensusProtobufReaderInternal implements InputEntityReader
  {
    @Override
    public CloseableIterator<InputRow> read()
    {
      Supplier<Iterator<InputRow>> supplier = Suppliers.memoize(() -> readAsList().iterator());
      return CloseableIterators.withEmptyBaggage(new Iterator<InputRow>() {
        @Override
        public boolean hasNext()
        {
          return supplier.get().hasNext();
        }
        @Override
        public InputRow next()
        {
          return supplier.get().next();
        }
      });
    }

    @Override
    public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
    {
      return OpenCensusProtobufReader.this.sample();
    }
  }

  public InputEntityReader newReader(ProtobufReader which)
  {
    switch(which) {
      case OPEN_TELEMETRY:
        return new OpenTelemetryMetricsProtobufReader(
            dimensionsSpec,
            source,
            metricDimension,
            valueDimension,
            metricLabelPrefix,
            resourceLabelPrefix
        );
      case OPEN_CENSUS:
      default:
        return new OpenCensusProtobufReaderInternal();
    }
  }

  public ProtobufReader whichReader()
  {
    // assume InputEntity is always defined in a single classloader (the kafka-indexing-service classloader)
    // so we only have to look it up once. To be completely correct we should cache the method based on classloader
    MethodHandle getHeaderMethod = KafkaUtils.lookupGetHeaderMethod(
        source.getEntity().getClass().getClassLoader(),
        VERSION_HEADER_KEY
    );

    try {
      byte[] versionHeader = (byte[]) getHeaderMethod.invoke(source.getEntity());
      if (versionHeader != null) {
        int version =
            ByteBuffer.wrap(versionHeader).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (version == OPENTELEMETRY_FORMAT_VERSION) {
          return ProtobufReader.OPEN_TELEMETRY;
        }
      }
    }
    catch (Throwable t) {
      // assume input is opencensus if something went wrong
    }
    return OPEN_CENSUS;
  }

  List<InputRow> readAsList()
  {
    try {
      return parseMetric(Metric.parseFrom(source.open()));
    }
    catch (IOException e) {
      throw new ParseException(null, e, "Protobuf message could not be parsed");
    }
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

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}

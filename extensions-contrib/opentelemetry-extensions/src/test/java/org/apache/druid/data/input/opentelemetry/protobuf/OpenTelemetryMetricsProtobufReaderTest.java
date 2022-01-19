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

import com.google.common.collect.ImmutableList;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;

public class OpenTelemetryMetricsProtobufReaderTest
{
  private static final long TIMESTAMP = System.nanoTime();
  public static final String RESOURCE_ATTRIBUTE_COUNTRY = "country";
  public static final String RESOURCE_ATTRIBUTE_VALUE_USA = "usa";

  public static final String RESOURCE_ATTRIBUTE_ENV = "env";
  public static final String RESOURCE_ATTRIBUTE_VALUE_DEVEL = "devel";

  public static final String INSTRUMENTATION_LIBRARY_NAME = "mock-instr-lib";
  public static final String INSTRUMENTATION_LIBRARY_VERSION = "1.0";

  public static final String METRIC_ATTRIBUTE_COLOR = "color";
  public static final String METRIC_ATTRIBUTE_VALUE_RED = "red";

  public static final String METRIC_ATTRIBUTE_FOO_KEY = "foo_key";
  public static final String METRIC_ATTRIBUTE_FOO_VAL = "foo_value";

  private final MetricsData.Builder metricsDataBuilder = MetricsData.newBuilder();

  private final Metric.Builder metricBuilder = metricsDataBuilder.addResourceMetricsBuilder()
          .addInstrumentationLibraryMetricsBuilder()
          .addMetricsBuilder();

  private final DimensionsSpec dimensionsSpec = new DimensionsSpec(ImmutableList.of(
          new StringDimensionSchema("descriptor." + METRIC_ATTRIBUTE_COLOR),
          new StringDimensionSchema("descriptor." + METRIC_ATTRIBUTE_FOO_KEY),
          new StringDimensionSchema("custom." + RESOURCE_ATTRIBUTE_ENV),
          new StringDimensionSchema("custom." + RESOURCE_ATTRIBUTE_COUNTRY)
  ), null, null);

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp()
  {
    metricsDataBuilder
            .getResourceMetricsBuilder(0)
            .getResourceBuilder()
            .addAttributes(
                    KeyValue.newBuilder()
                            .setKey(RESOURCE_ATTRIBUTE_COUNTRY)
                            .setValue(AnyValue.newBuilder().setStringValue(RESOURCE_ATTRIBUTE_VALUE_USA))
                            .build());

    metricsDataBuilder
            .getResourceMetricsBuilder(0)
            .getInstrumentationLibraryMetricsBuilder(0)
            .getInstrumentationLibraryBuilder()
            .setName(INSTRUMENTATION_LIBRARY_NAME)
            .setVersion(INSTRUMENTATION_LIBRARY_VERSION);

  }

  @Test
  public void testIntSumParse()
  {
    metricBuilder.setName("example_int_sum")
            .getIntSumBuilder()
            .addDataPointsBuilder()
            .setValue(3L)
            .setTimeUnixNano(TIMESTAMP)
            .addLabelsBuilder().setKey(METRIC_ATTRIBUTE_COLOR).setValue(METRIC_ATTRIBUTE_VALUE_RED);

    MetricsData metricsData = metricsDataBuilder.build();

    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
            dimensionsSpec,
            new ByteEntity(metricsData.toByteArray()),
            "metric.name",
            null,
            "descriptor.",
            "custom."
    ).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_int_sum");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
  }

  @Test
  public void testIntGaugeParse()
  {
    metricBuilder.setName("example_int_gauge")
            .getIntGaugeBuilder()
            .addDataPointsBuilder()
            .setValue(3L)
            .setTimeUnixNano(TIMESTAMP)
            .addLabelsBuilder().setKey(METRIC_ATTRIBUTE_COLOR).setValue(METRIC_ATTRIBUTE_VALUE_RED);

    MetricsData metricsData = metricsDataBuilder.build();

    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
            dimensionsSpec,
            new ByteEntity(metricsData.toByteArray()),
            "metric.name",
            null,
            "descriptor.",
            "custom."
    ).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_int_gauge");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
  }

  @Test
  public void testSumWithAttributes()
  {
    metricBuilder
            .setName("example_sum")
            .getSumBuilder()
            .addDataPointsBuilder()
            .setAsInt(6)
            .setTimeUnixNano(TIMESTAMP)
            .addAttributesBuilder() // test sum with attributes
            .setKey(METRIC_ATTRIBUTE_COLOR)
            .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());

    MetricsData metricsData = metricsDataBuilder.build();

    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
            dimensionsSpec,
            new ByteEntity(metricsData.toByteArray()),
            "metric.name",
            null,
            "descriptor.",
            "custom."
    ).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_sum");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
  }

  @Test
  public void testGaugeWithAttributes()
  {
    metricBuilder.setName("example_gauge")
            .getGaugeBuilder()
            .addDataPointsBuilder()
            .setAsInt(6)
            .setTimeUnixNano(TIMESTAMP)
            .addAttributesBuilder() // test sum with attributes
            .setKey(METRIC_ATTRIBUTE_COLOR)
            .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());

    MetricsData metricsData = metricsDataBuilder.build();

    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
            dimensionsSpec,
            new ByteEntity(metricsData.toByteArray()),
            "metric.name",
            null,
            "descriptor.",
            "custom."
    ).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_gauge");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");
  }

  @Test
  public void testBatchedMetricParse()
  {
    metricBuilder.setName("example_sum")
            .getSumBuilder()
            .addDataPointsBuilder()
            .setAsInt(6)
            .setTimeUnixNano(TIMESTAMP)
            .addAttributesBuilder() // test sum with attributes
            .setKey(METRIC_ATTRIBUTE_COLOR)
            .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());

    // Create Second Metric
    Metric.Builder gaugeMetricBuilder = metricsDataBuilder.addResourceMetricsBuilder()
            .addInstrumentationLibraryMetricsBuilder()
            .addMetricsBuilder();

    metricsDataBuilder.getResourceMetricsBuilder(1)
            .getResourceBuilder()
            .addAttributes(
                    KeyValue.newBuilder()
                            .setKey(RESOURCE_ATTRIBUTE_ENV)
                            .setValue(AnyValue.newBuilder().setStringValue(RESOURCE_ATTRIBUTE_VALUE_DEVEL))
                            .build());

    metricsDataBuilder.getResourceMetricsBuilder(1)
            .getInstrumentationLibraryMetricsBuilder(0)
            .getInstrumentationLibraryBuilder()
            .setName(INSTRUMENTATION_LIBRARY_NAME)
            .setVersion(INSTRUMENTATION_LIBRARY_VERSION);

    gaugeMetricBuilder.setName("example_gauge")
            .getGaugeBuilder()
            .addDataPointsBuilder()
            .setAsInt(6)
            .setTimeUnixNano(TIMESTAMP)
            .addAttributesBuilder() // test sum with attributes
            .setKey(METRIC_ATTRIBUTE_FOO_KEY)
            .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_FOO_VAL).build());

    MetricsData metricsData = metricsDataBuilder.build();

    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
            dimensionsSpec,
            new ByteEntity(metricsData.toByteArray()),
            "metric.name",
            null,
            "descriptor.",
            "custom."
    ).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(2, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_sum");
    assertDimensionEquals(row, "custom.country", "usa");
    assertDimensionEquals(row, "descriptor.color", "red");

    row = rowList.get(1);
    Assert.assertEquals(4, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_gauge");
    assertDimensionEquals(row, "custom.env", "devel");
    assertDimensionEquals(row, "descriptor.foo_key", "foo_value");

  }

  @Test
  public void testDimensionSpecExclusions()
  {
    metricBuilder.setName("example_gauge")
            .getGaugeBuilder()
            .addDataPointsBuilder()
            .setAsInt(6)
            .setTimeUnixNano(TIMESTAMP)
            .addAttributesBuilder() // test sum with attributes
            .setKey(METRIC_ATTRIBUTE_COLOR)
            .setValue(AnyValue.newBuilder().setStringValue(METRIC_ATTRIBUTE_VALUE_RED).build());

    MetricsData metricsData = metricsDataBuilder.build();

    DimensionsSpec dimensionsSpecWithExclusions = new DimensionsSpec(null,
            ImmutableList.of(
                    "descriptor." + METRIC_ATTRIBUTE_COLOR,
                    "custom." + RESOURCE_ATTRIBUTE_COUNTRY
            ), null);

    CloseableIterator<InputRow> rows = new OpenTelemetryMetricsProtobufReader(
            dimensionsSpecWithExclusions,
            new ByteEntity(metricsData.toByteArray()),
            "metric.name",
            null,
            "descriptor.",
            "custom."
    ).read();

    List<InputRow> rowList = new ArrayList<>();
    rows.forEachRemaining(rowList::add);
    Assert.assertEquals(1, rowList.size());

    InputRow row = rowList.get(0);
    Assert.assertEquals(2, row.getDimensions().size());
    assertDimensionEquals(row, "metric.name", "example_gauge");

    List<String> values = row.getDimension("custom.country");
    Assert.assertEquals(0, values.size());

    values = row.getDimension("descriptor.color");
    Assert.assertEquals(0, values.size());
  }

  private void assertDimensionEquals(InputRow row, String dimension, Object expected)
  {
    List<String> values = row.getDimension(dimension);
    Assert.assertEquals(1, values.size());
    Assert.assertEquals(expected, values.get(0));
  }

}

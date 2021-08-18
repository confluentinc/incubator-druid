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

package org.apache.druid.data.input.kafkainput;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.DateTimes;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class KafkaInputFormat implements InputFormat
{
  private static final String DEFAULT_HEADER_COLUMN_PREFIX = "kafka.header.";
  private static final String DEFAULT_TIMESTAMP_COLUMN_PREFIX = "kafka.";
  private static final String DEFAULT_KEY_COLUMN_PREFIX = "kafka.";
  private static final String DEFAULT_TIMESTAMP_STRING = "timestamp";
  private static final String DEFAULT_KEY_STRING = "key";
  public static final String DEFAULT_AUTO_TIMESTAMP_STRING = "__kif_auto_timestamp";

  // Since KafkaInputFormat blends data from header, key and payload, timestamp spec can be pointing to an attribute within one of these
  // 3 sections. To handle scenarios where there is no timestamp value either in key or payload, we induce an artifical timestamp value
  // to avoid unnecessary parser barf out. Users in such situations can use the inputFormat's kafka record timestamp as its primary timestamp.
  private final TimestampSpec dummyTimestampSpec = new TimestampSpec(DEFAULT_AUTO_TIMESTAMP_STRING, "auto", DateTimes.EPOCH);

  private final KafkaHeaderFormat headerFormat;
  private final InputFormat valueFormat;
  private final InputFormat keyFormat;
  private final String headerColumnPrefix;
  private final String keyColumnPrefix;
  private final String recordTimestampColumnPrefix;

  public KafkaInputFormat(
      @JsonProperty("headerFormat") @Nullable KafkaHeaderFormat headerFormat,
      @JsonProperty("keyFormat") @Nullable InputFormat keyFormat,
      @JsonProperty("valueFormat") InputFormat valueFormat,
      @JsonProperty("headerColumnPrefix") @Nullable String headerColumnPrefix,
      @JsonProperty("keyColumnPrefix") @Nullable String keyColumnPrefix,
      @JsonProperty("recordTimestampColumnPrefix") @Nullable String recordTimestampColumnPrefix
  )
  {
    this.headerFormat = headerFormat;
    this.keyFormat = keyFormat;
    this.valueFormat = Preconditions.checkNotNull(valueFormat, "valueFormat must not be null");
    this.headerColumnPrefix = headerColumnPrefix != null ? headerColumnPrefix : DEFAULT_HEADER_COLUMN_PREFIX;
    this.keyColumnPrefix = keyColumnPrefix != null ? keyColumnPrefix : DEFAULT_KEY_COLUMN_PREFIX;
    this.recordTimestampColumnPrefix = recordTimestampColumnPrefix != null ? recordTimestampColumnPrefix : DEFAULT_TIMESTAMP_COLUMN_PREFIX;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    KafkaRecordEntity record = (KafkaRecordEntity) source;
    InputRowSchema newInputRowSchema = new InputRowSchema(dummyTimestampSpec, inputRowSchema.getDimensionsSpec(), inputRowSchema.getMetricNames());
    return new KafkaInputReader(
        inputRowSchema,
        record,
        (headerFormat == null) ?
          null :
          headerFormat.createReader(record.getRecord().headers(), headerColumnPrefix),
        (keyFormat == null || record.getRecord().key() == null) ?
          null :
          keyFormat.createReader(
                  newInputRowSchema,
                  new ByteEntity(record.getRecord().key()),
                  temporaryDirectory
          ),
        (record.getRecord().value() == null) ?
          null :
          valueFormat.createReader(
                  newInputRowSchema,
                  source,
                  temporaryDirectory
          ),
        keyColumnPrefix + DEFAULT_KEY_STRING,
        recordTimestampColumnPrefix + DEFAULT_TIMESTAMP_STRING
    );
  }

  @JsonProperty
  public KafkaHeaderFormat getHeaderFormat()
  {
    return headerFormat;
  }

  @JsonProperty
  public InputFormat getValueFormat()
  {
    return valueFormat;
  }

  @JsonProperty
  public InputFormat getKeyFormat()
  {
    return keyFormat;
  }

  @JsonProperty
  public String getHeaderColumnPrefix()
  {
    return headerColumnPrefix;
  }

  @JsonProperty
  public String getKeyColumnPrefix()
  {
    return keyColumnPrefix;
  }

  @JsonProperty
  public String getRecordTimestampColumnPrefix()
  {
    return recordTimestampColumnPrefix;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaInputFormat)) {
      return false;
    }
    KafkaInputFormat that = (KafkaInputFormat) o;
    return Objects.equals(headerFormat, that.headerFormat)
           && Objects.equals(valueFormat, that.valueFormat)
           && Objects.equals(keyFormat, that.keyFormat)
           && Objects.equals(headerColumnPrefix, that.headerColumnPrefix)
           && Objects.equals(keyColumnPrefix, that.keyColumnPrefix)
           && Objects.equals(recordTimestampColumnPrefix, that.recordTimestampColumnPrefix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(headerFormat, valueFormat, keyFormat,
                        headerColumnPrefix, keyColumnPrefix, recordTimestampColumnPrefix
    );
  }
}

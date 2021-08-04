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
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;
import java.util.Objects;

public class KafkaInputFormat implements InputFormat
{
  private static final String DEFAULT_HEADER_LABEL_PREFIX = "kafka.header.";
  private static final String DEFAULT_TIMESTAMP_LABEL_PREFIX = "kafka.";
  private static final String DEFAULT_KEY_LABEL_PREFIX = "kafka.";

  private final KafkaHeaderFormat headerReader;
  private final InputFormat valueFormat;
  private final InputFormat keyFormat;
  private final String headerLabelPrefix;
  private final String keyLabelPrefix;
  private final String recordTimestampLabelPrefix;

  public KafkaInputFormat(
      @JsonProperty("headerFormat") KafkaHeaderFormat headerReader,
      @JsonProperty("keyFormat") InputFormat keyFormat,
      @JsonProperty("valueFormat") InputFormat valueFormat,
      @JsonProperty("headerLabelPrefix") @Nullable String headerLabelPrefix,
      @JsonProperty("keyLabelPrefix") @Nullable String keyLabelPrefix,
      @JsonProperty("recordTimestampLabelPrefix") @Nullable String recordTimestampLabelPrefix
  )
  {
    this.headerReader = Preconditions.checkNotNull(headerReader, "headerFormat");
    this.keyFormat = Preconditions.checkNotNull(keyFormat, "keyFormat");
    this.valueFormat = Preconditions.checkNotNull(valueFormat, "valueFormat");
    this.headerLabelPrefix = headerLabelPrefix != null ? headerLabelPrefix : DEFAULT_HEADER_LABEL_PREFIX;
    this.keyLabelPrefix = keyLabelPrefix != null ? keyLabelPrefix : DEFAULT_KEY_LABEL_PREFIX;
    this.recordTimestampLabelPrefix = recordTimestampLabelPrefix != null ? recordTimestampLabelPrefix : DEFAULT_TIMESTAMP_LABEL_PREFIX;
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
    // Since KafkaInputFormat blends data from header, key and payload, timestamp spec can be pointing to an attribute within one of these
    // 3 sections. To handle scenarios where there is no timestamp value either in key or payload, we induce an artifical timestamp value
    // to avoid unnecessary parser barf out. Users in such situations can use the inputFormat's kafka record timestamp as its primary timestamp.
    TimestampSpec dummyTimestampSpec = new TimestampSpec("__kif_auto_timestamp", "auto", DateTimes.EPOCH);
    InputRowSchema newInputRowSchema = new InputRowSchema(dummyTimestampSpec, inputRowSchema.getDimensionsSpec(), inputRowSchema.getMetricNames());
    return new KafkaInputReader(
            inputRowSchema,
            record,
            headerReader.createReader(record.getRecord().headers(), headerLabelPrefix),
            (record.getRecord().key() != null) ?
            keyFormat.createReader(
                    newInputRowSchema,
                    new ByteEntity(record.getRecord().key()),
                    temporaryDirectory
            ) : null,
            (record.getRecord().value() != null) ?
            valueFormat.createReader(
                    newInputRowSchema,
                    source,
                    temporaryDirectory
            ) : null,
            keyLabelPrefix,
            recordTimestampLabelPrefix
    );
  }

  @JsonProperty
  public KafkaHeaderFormat getHeaderFormat()
  {
    return headerReader;
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
  public String getHeaderLabelPrefix()
  {
    return headerLabelPrefix;
  }

  @JsonProperty
  public String getKeyLabelPrefix()
  {
    return keyLabelPrefix;
  }

  @JsonProperty
  public String getRecordTimestampLabelPrefix()
  {
    return recordTimestampLabelPrefix;
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
    return Objects.equals(headerReader, that.headerReader)
           && Objects.equals(valueFormat, that.valueFormat)
           && Objects.equals(keyFormat, that.keyFormat)
           && Objects.equals(headerLabelPrefix, that.headerLabelPrefix)
           && Objects.equals(keyLabelPrefix, that.keyLabelPrefix)
           && Objects.equals(recordTimestampLabelPrefix, that.recordTimestampLabelPrefix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(headerReader, valueFormat, keyFormat,
                        headerLabelPrefix, keyLabelPrefix, recordTimestampLabelPrefix);
  }
}

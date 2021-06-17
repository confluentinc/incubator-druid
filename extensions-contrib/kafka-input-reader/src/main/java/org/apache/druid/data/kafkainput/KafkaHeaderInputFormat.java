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

package org.apache.druid.data.kafkainput;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;

import java.io.File;
import java.util.Objects;

public class KafkaHeaderInputFormat implements InputFormat {
  private static final String DEFAULT_VALUE_TYPE = "String";
  private static final String DEFAULT_HEADER_LABEL_PREFIX = "kafka.header.";

  private final String valueType;
  private final String headerLabelPrefix;

  @JsonCreator
  public KafkaHeaderInputFormat(
          @JsonProperty("valueType") String valueType,
          @JsonProperty("headerLabelPrefix") String headerLabelPrefix
  ) {
    this.valueType = valueType == null ? DEFAULT_VALUE_TYPE : valueType;
    this.headerLabelPrefix = headerLabelPrefix == null ? DEFAULT_HEADER_LABEL_PREFIX : headerLabelPrefix;
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory) {
    return new KafkaHeaderReader(
            (KafkaRecordEntity) source,
            this.headerLabelPrefix
    );
  }

  @JsonProperty
  public String getValueType() {
    return valueType;
  }

  @JsonProperty
  public String getHeaderLabelPrefix() {
    return headerLabelPrefix;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KafkaHeaderInputFormat that = (KafkaHeaderInputFormat) o;
    return Objects.equals(valueType, that.valueType)
            && Objects.equals(headerLabelPrefix, that.headerLabelPrefix);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueType, headerLabelPrefix);
  }
}
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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.ByteEntity;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;

import java.io.File;
import java.util.Objects;

public class KafkaInputFormat implements InputFormat
{
  private final KafkaHeaderInputFormat headerFormat;
  private final PayloadValueInputFormat payloadValueFormat;
  private final PayloadKeyInputFormat payloadKeyFormat;

  public KafkaInputFormat(
      @JsonProperty("headerFormat") @Nullable KafkaHeaderInputFormat headerFormat,
      @JsonProperty("payloadValueFormat") @Nullable PayloadValueInputFormat payloadValueFormat,
      @JsonProperty("payloadKeyFormat") @Nullable PayloadKeyInputFormat payloadKeyFormat
  )
  {
    this.headerFormat = Preconditions.checkNotNull(headerFormat, "headerFormat");
    this.payloadValueFormat = Preconditions.checkNotNull(payloadValueFormat, "payloadValueFormat");
    this.payloadKeyFormat = Preconditions.checkNotNull(payloadKeyFormat, "payloadKeyFormat");
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    //return null;
    return this.headerFormat.createReader(inputRowSchema, source, temporaryDirectory);
  }

  @JsonProperty
  public KafkaHeaderInputFormat getHeaderFormat()
  {
    return headerFormat;
  }

  @JsonProperty
  public PayloadValueInputFormat getPayloadValueFormat()
  {
    return payloadValueFormat;
  }

  @JsonProperty
  public PayloadKeyInputFormat getPayloadKeyFormat()
  {
    return payloadKeyFormat;
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
           && Objects.equals(payloadValueFormat, that.payloadValueFormat)
           && Objects.equals(payloadKeyFormat, that.payloadKeyFormat);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(headerFormat, payloadValueFormat, payloadKeyFormat);
  }
}

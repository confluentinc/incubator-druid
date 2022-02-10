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
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.java.util.common.Pair;

import java.util.List;

public class KafkaHybridFormatsRule
{

  private final String headerRegex;
  private final InputFormat valueFormat;

  public KafkaHybridFormatsRule(
      @JsonProperty("headerRegex") String headerRegex,
      @JsonProperty("valueFormat") InputFormat valueFormat
  )
  {
    this.headerRegex = Preconditions.checkNotNull(headerRegex,
      "kafkaHybridFormatsRule.headerRegex must not be null");
    this.valueFormat = Preconditions.checkNotNull(valueFormat,
      "kafkaHybridFormatsRule.valueFormat must not be null");
  }

  public boolean match(List<Pair<String, Object>> headers)
  {
    if (headers != null && !headers.isEmpty()) {
      return headers.stream().anyMatch(header -> header.toString().matches(this.headerRegex));
    }
    return false;
  }

  public InputFormat getValueFormat()
  {
    return this.valueFormat;
  }
}

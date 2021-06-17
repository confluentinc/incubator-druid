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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.kafka.common.header.Headers;

import javax.annotation.Nullable;
import java.util.Objects;

public class KafkaStringHeaderFormat implements KafkaHeaderFormat
{
  private static final Logger log = new Logger(KafkaStringHeaderFormat.class);
  private static final String DEFAULT_STRING_ENCODING = "UTF8";
  private final String encoding;

  public KafkaStringHeaderFormat(
      @JsonProperty("encoding") @Nullable String encoding
  )
  {
    this.encoding = (encoding != null) ? encoding : DEFAULT_STRING_ENCODING;
  }

  @JsonProperty
  public String getEncoding()
  {
    return encoding;
  }

  @Override
  public KafkaHeaderReader createReader(
      Headers headers,
      String headerLabelPrefix
  )
  {
    return new KafkaStringHeaderReader(
        headers,
        this.encoding,
        headerLabelPrefix
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaStringHeaderFormat)) {
      return false;
    }
    KafkaStringHeaderFormat that = (KafkaStringHeaderFormat) o;
    return Objects.equals(encoding, that.encoding);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(encoding);
  }

}

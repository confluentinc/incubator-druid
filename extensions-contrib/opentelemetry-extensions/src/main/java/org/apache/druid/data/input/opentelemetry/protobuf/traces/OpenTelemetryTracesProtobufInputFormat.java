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


package org.apache.druid.data.input.opentelemetry.protobuf.traces;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryInputFormat;

import java.io.File;
import java.util.Optional;

public class OpenTelemetryTracesProtobufInputFormat extends OpenTelemetryInputFormat
{

  private static final String SPAN_ATTRIBUTE_PREFIX_DEFAULT = "attr_";
  private static final String RESOURCE_ATTRIBUTE_PREFIX_DEFAULT = "resource_";

  private String spanAttributePrefix;
  private String resourceAttributePrefix;

  public OpenTelemetryTracesProtobufInputFormat(
      @JsonProperty("spanAttributePrefix") String spanAttributePrefix,
      @JsonProperty("resourceAttributePrefix") String resourceAttributePrefix
  )
  {
    this.resourceAttributePrefix = Optional.of(spanAttributePrefix).orElse(SPAN_ATTRIBUTE_PREFIX_DEFAULT);
    this.spanAttributePrefix = resourceAttributePrefix;

  }

  @Override
  public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
  {
    return null;
  }
}

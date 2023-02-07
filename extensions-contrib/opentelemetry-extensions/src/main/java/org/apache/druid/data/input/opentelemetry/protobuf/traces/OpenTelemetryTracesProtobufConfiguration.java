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

import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Objects;

public class OpenTelemetryTracesProtobufConfiguration
{
  private final String resourceAttributePrefix;
  private final String spanAttributePrefix;

  public static final int DEFAULT_COLUMN_COUNT = 8;

  private final String nameDimension;
  private final String spanIdDimension;
  private final String parentSpanIdDimension;
  private final String traceIdDimension;
  private final String endTimeDimension;
  private final String statusCodeDimension;
  private final String statusMessageDimension;
  private final String kindDimension;


  @JsonProperty
  public String getNameDimension()
  {
    return nameDimension;
  }

  @JsonProperty
  public String getResourceAttributePrefix()
  {
    return resourceAttributePrefix;
  }

  @JsonProperty
  public String getSpanIdDimension()
  {
    return spanIdDimension;
  }

  @JsonProperty
  public String getParentSpanIdDimension()
  {
    return parentSpanIdDimension;
  }

  @JsonProperty
  public String getTraceIdDimension()
  {
    return traceIdDimension;
  }

  @JsonProperty
  public String getEndTimeDimension()
  {
    return endTimeDimension;
  }

  @JsonProperty
  public String getStatusCodeDimension()
  {
    return statusCodeDimension;
  }

  @JsonProperty
  public String getStatusMessageDimension()
  {
    return statusMessageDimension;
  }

  @JsonProperty
  public String getKindDimension()
  {
    return kindDimension;
  }

  @JsonProperty
  public String getSpanAttributePrefix()
  {
    return spanAttributePrefix;
  }

  private OpenTelemetryTracesProtobufConfiguration(
      @JsonProperty("resourceAttributePrefix") String resourceAttributePrefix,
      @JsonProperty("spanAttributePrefix") String spanAttributePrefix,
      @JsonProperty("nameDimension") String nameDimension,
      @JsonProperty("spanIdDimension") String spanIdDimension,
      @JsonProperty("parentSpanIdDimension") String parentSpanIdDimension,
      @JsonProperty("traceIdDimension") String traceIdDimension,
      @JsonProperty("endTimeDimension") String endTimeDimension,
      @JsonProperty("statusCodeDimension") String statusCodeDimension,
      @JsonProperty("statusMessageDimension") String statusMessageDimension,
      @JsonProperty("kindDimension") String kindDimension
  )
  {
    this.resourceAttributePrefix = resourceAttributePrefix;
    this.spanAttributePrefix = spanAttributePrefix;
    this.nameDimension = nameDimension;
    this.spanIdDimension = spanIdDimension;
    this.parentSpanIdDimension = parentSpanIdDimension;
    this.traceIdDimension = traceIdDimension;
    this.endTimeDimension = endTimeDimension;
    this.statusCodeDimension = statusCodeDimension;
    this.statusMessageDimension = statusMessageDimension;
    this.kindDimension = kindDimension;
  }

  public static Builder newBuilder()
  {
    return new Builder();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OpenTelemetryTracesProtobufConfiguration that = (OpenTelemetryTracesProtobufConfiguration) o;
    return Objects.equals(spanAttributePrefix, that.spanAttributePrefix)
           && Objects.equals(nameDimension, that.nameDimension)
           && Objects.equals(spanIdDimension, that.spanIdDimension)
           && Objects.equals(parentSpanIdDimension, that.parentSpanIdDimension)
           && Objects.equals(traceIdDimension, that.traceIdDimension)
           && Objects.equals(endTimeDimension, that.endTimeDimension)
           && Objects.equals(statusCodeDimension, that.statusCodeDimension)
           && Objects.equals(statusMessageDimension, that.statusMessageDimension)
           && Objects.equals(kindDimension, that.kindDimension)
           && Objects.equals(resourceAttributePrefix, that.resourceAttributePrefix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        spanAttributePrefix,
        nameDimension,
        spanIdDimension,
        parentSpanIdDimension,
        traceIdDimension,
        endTimeDimension,
        statusCodeDimension,
        statusMessageDimension,
        kindDimension,
        resourceAttributePrefix
    );
  }

  public static class Builder
  {
    private String spanAttributePrefix = "";
    private String resourceAttributePrefix = "resource.";
    private String nameDimension = "name";
    private String spanIdDimension = "span.id";
    private String parentSpanIdDimension = "parent.span.id";
    private String traceIdDimension = "trace.id";
    private String endTimeDimension = "end.time";
    private String statusCodeDimension = "status.code";
    private String statusMessageDimension = "status.message";
    private String kindDimension = "kind";

    private Builder()
    {
    }

    public Builder setSpanAttributePrefix(String spanAttributePrefix)
    {
      Objects.requireNonNull(spanAttributePrefix, "Span attribute prefix cannot be null");
      this.spanAttributePrefix = spanAttributePrefix;
      return this;
    }

    public Builder setResourceAttributePrefix(String resourceAttributePrefix)
    {
      Objects.requireNonNull(resourceAttributePrefix, "Resource attribute prefix cannot be null");
      this.resourceAttributePrefix = resourceAttributePrefix;
      return this;
    }

    private void throwIfNullOrEmpty(String input, String dimensionName)
    {
      Preconditions.checkArgument(!StringUtils.isNullOrEmpty(input),
                                  dimensionName + " dimension cannot be null or empty");
    }

    public Builder setNameDimension(String name)
    {
      throwIfNullOrEmpty(name, "Name");
      this.nameDimension = name;
      return this;
    }

    public Builder setSpanIdDimension(String spanId)
    {
      throwIfNullOrEmpty(spanId, "Span Id");
      this.spanIdDimension = spanId;
      return this;
    }

    public Builder setParentSpanIdDimension(String parentSpanId)
    {
      throwIfNullOrEmpty(parentSpanId, "Parent Span Id");
      this.parentSpanIdDimension = parentSpanId;
      return this;
    }

    public Builder setTraceIdDimension(String traceId)
    {
      throwIfNullOrEmpty(traceId, "Trace Id");
      this.traceIdDimension = traceId;
      return this;
    }

    public Builder setEndTimeDimension(String endTime)
    {
      throwIfNullOrEmpty(endTime, "End Time");
      this.endTimeDimension = endTime;
      return this;
    }

    public Builder setStatusCodeDimension(String statusCode)
    {
      throwIfNullOrEmpty(statusCode, "Status Code");
      this.statusCodeDimension = statusCode;
      return this;
    }

    public Builder setStatusMessageDimension(String statusMessage)
    {
      throwIfNullOrEmpty(statusMessage, "Status Message");
      this.statusMessageDimension = statusMessage;
      return this;
    }

    public Builder setKindDimension(String kind)
    {
      throwIfNullOrEmpty(kind, "Kind");
      this.kindDimension = kind;
      return this;
    }

    public OpenTelemetryTracesProtobufConfiguration build()
    {
      return new OpenTelemetryTracesProtobufConfiguration(
          resourceAttributePrefix,
          spanAttributePrefix,
          nameDimension,
          spanIdDimension,
          parentSpanIdDimension,
          traceIdDimension,
          endTimeDimension,
          statusCodeDimension,
          statusMessageDimension,
          kindDimension
      );
    }
  }
}

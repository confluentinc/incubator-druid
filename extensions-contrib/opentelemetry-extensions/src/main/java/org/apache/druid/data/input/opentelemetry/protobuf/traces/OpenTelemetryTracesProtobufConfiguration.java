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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Preconditions;

import java.util.Objects;

@JsonDeserialize(builder = OpenTelemetryTracesProtobufConfiguration.Builder.class)
public class OpenTelemetryTracesProtobufConfiguration
{

  private final String resourceAttributePrefix;
  private final String spanAttributePrefix;

  // Number of '*Dimension' variables
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
      Builder builder
  )
  {
    this.resourceAttributePrefix = builder.resourceAttributePrefix;
    this.spanAttributePrefix = builder.spanAttributePrefix;
    this.nameDimension = builder.nameDimension;
    this.spanIdDimension = builder.spanIdDimension;
    this.parentSpanIdDimension = builder.parentSpanIdDimension;
    this.traceIdDimension = builder.traceIdDimension;
    this.endTimeDimension = builder.endTimeDimension;
    this.statusCodeDimension = builder.statusCodeDimension;
    this.statusMessageDimension = builder.statusMessageDimension;
    this.kindDimension = builder.kindDimension;
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

  @JsonPOJOBuilder(withPrefix = "set")
  public static class Builder
  {
    @JsonProperty("spanAttributePrefix")
    private String spanAttributePrefix = "span.";

    @JsonProperty("resourceAttributePrefix")
    private String resourceAttributePrefix = "resource.";

    @JsonProperty("nameDimension")
    private String nameDimension = "name";

    @JsonProperty("spanIdDimension")
    private String spanIdDimension = "span.id";

    @JsonProperty("parentSpanIdDimension")
    private String parentSpanIdDimension = "parent.span.id";

    @JsonProperty("traceIdDimension")
    private String traceIdDimension = "trace.id";

    @JsonProperty("endTimeDimension")
    private String endTimeDimension = "end.time";

    @JsonProperty("statusCodeDimension")
    private String statusCodeDimension = "status.code";

    @JsonProperty("statusMessageDimension")
    private String statusMessageDimension = "status.message";

    @JsonProperty("kindDimension")
    private String kindDimension = "kind";

    private Builder()
    {
    }

    @JsonProperty
    public Builder setSpanAttributePrefix(String spanAttributePrefix)
    {
      Preconditions.checkNotNull(spanAttributePrefix, "Span attribute prefix cannot be null");
      this.spanAttributePrefix = spanAttributePrefix;
      return this;
    }

    @JsonProperty
    public Builder setResourceAttributePrefix(String resourceAttributePrefix)
    {
      Preconditions.checkNotNull(resourceAttributePrefix, "Resource attribute prefix cannot be null");
      this.resourceAttributePrefix = resourceAttributePrefix;
      return this;
    }

    @JsonProperty
    private void throwIfNullOrEmpty(String input, String dimensionName)
    {
      Preconditions.checkArgument(!(input == null || input.isEmpty()),
                                  dimensionName + " dimension cannot be null or empty");
    }

    @JsonProperty
    public Builder setNameDimension(String name)
    {
      throwIfNullOrEmpty(name, "Name");
      this.nameDimension = name;
      return this;
    }

    @JsonProperty
    public Builder setSpanIdDimension(String spanId)
    {
      throwIfNullOrEmpty(spanId, "Span Id");
      this.spanIdDimension = spanId;
      return this;
    }

    @JsonProperty
    public Builder setParentSpanIdDimension(String parentSpanId)
    {
      throwIfNullOrEmpty(parentSpanId, "Parent Span Id");
      this.parentSpanIdDimension = parentSpanId;
      return this;
    }

    @JsonProperty
    public Builder setTraceIdDimension(String traceId)
    {
      throwIfNullOrEmpty(traceId, "Trace Id");
      this.traceIdDimension = traceId;
      return this;
    }

    @JsonProperty
    public Builder setEndTimeDimension(String endTime)
    {
      throwIfNullOrEmpty(endTime, "End Time");
      this.endTimeDimension = endTime;
      return this;
    }

    @JsonProperty
    public Builder setStatusCodeDimension(String statusCode)
    {
      throwIfNullOrEmpty(statusCode, "Status Code");
      this.statusCodeDimension = statusCode;
      return this;
    }

    @JsonProperty
    public Builder setStatusMessageDimension(String statusMessage)
    {
      throwIfNullOrEmpty(statusMessage, "Status Message");
      this.statusMessageDimension = statusMessage;
      return this;
    }

    @JsonProperty
    public Builder setKindDimension(String kind)
    {
      throwIfNullOrEmpty(kind, "Kind");
      this.kindDimension = kind;
      return this;
    }

    public OpenTelemetryTracesProtobufConfiguration build()
    {
      return new OpenTelemetryTracesProtobufConfiguration(
          this
      );
    }
  }
}

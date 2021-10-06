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

package org.apache.druid.emitter.opentelemetry;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class OpenTelemetryEmitterTest
{
  private static class NoopExporter implements SpanExporter
  {
    public Collection<SpanData> spanData;

    @Override
    public CompletableResultCode export(Collection<SpanData> collection)
    {
      this.spanData = collection;
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush()
    {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown()
    {
      return CompletableResultCode.ofSuccess();
    }
  }


  // Check that we don't call "emitQueryTimeEvent" method for event that is not instance of ServiceMetricEvent
  @Test
  public void testNoEmitNotServiceMetric()
  {
    final Event notServiceMetricEvent =
        new Event()
        {
          @Override
          public Map<String, Object> toMap()
          {
            return null;
          }

          @Override
          public String getFeed()
          {
            return null;
          }
        };

    OpenTelemetryEmitter emitter = EasyMock.createMock(OpenTelemetryEmitter.class);
    emitter.emit(notServiceMetricEvent);
    EasyMock.replay(emitter);

    emitter.emit(notServiceMetricEvent);
    EasyMock.verifyUnexpectedCalls(emitter);
  }

  // Check that we don't call "emitQueryTimeEvent" method for ServiceMetricEvent that is not "query/time" type
  @Test
  public void testNoEmitNotQueryTimeMetric()
  {
    final ServiceMetricEvent notQueryTimeMetric =
        new ServiceMetricEvent.Builder().build(
                                            DateTimes.nowUtc(),
                                            "query/cache/total/hitRate",
                                            0.54
                                        )
                                        .build(
                                            "broker",
                                            "brokerHost1"
                                        );

    OpenTelemetryEmitter emitter = EasyMock.createMock(OpenTelemetryEmitter.class);
    emitter.emit(notQueryTimeMetric);
    EasyMock.replay(emitter);

    emitter.emit(notQueryTimeMetric);
    EasyMock.verifyUnexpectedCalls(emitter);
  }

  @Test
  public void testTraceparentId()
  {
    final String traceId = "00-54ef39243e3feb12072e0f8a74c1d55a-ad6d5b581d7c29c1-01";
    final String expectedParentTraceId = "54ef39243e3feb12072e0f8a74c1d55a";
    final String expectedParentSpanId = "ad6d5b581d7c29c1";
    final Map<String, String> context = new HashMap<>();
    context.put("traceparent", traceId);

    final String serviceName = "druid/broker";
    final DateTime createdTime = DateTimes.nowUtc();
    final long metricValue = 100;

    NoopExporter noopExporter = new NoopExporter();
    SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                                                        .addSpanProcessor(SimpleSpanProcessor.create(noopExporter))
                                                        .build();

    final ServiceMetricEvent queryTimeMetric =
        new ServiceMetricEvent.Builder().setDimension("context", context)
                                        .build(
                                            createdTime,
                                            "query/time",
                                            metricValue
                                        )
                                        .build(
                                            serviceName,
                                            "host"
                                        );
    OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                    .buildAndRegisterGlobal();

    OpenTelemetryEmitter emitter = new OpenTelemetryEmitter();
    emitter.emit(queryTimeMetric);

    Assert.assertEquals(1, noopExporter.spanData.size());

    SpanData actualSpanData = noopExporter.spanData.iterator().next();
    Assert.assertEquals(serviceName, actualSpanData.getName());
    Assert.assertEquals((createdTime.getMillis() - metricValue) * 1_000_000, actualSpanData.getStartEpochNanos());
    Assert.assertEquals(expectedParentTraceId, actualSpanData.getParentSpanContext().getTraceId());
    Assert.assertEquals(expectedParentSpanId, actualSpanData.getParentSpanContext().getSpanId());
  }
}

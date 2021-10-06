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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OpenTelemetryEmitter implements Emitter
{
  private static final Logger log = new Logger(OpenTelemetryEmitter.class);
  private static final DruidContextTextMapGetter DRUID_CONTEXT_MAP_GETTER = new DruidContextTextMapGetter();
  private final Tracer tracer;
  private final TextMapPropagator propagator;

  OpenTelemetryEmitter()
  {
    tracer = GlobalOpenTelemetry.getTracer("druid-opentelemetry-extension");
    propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
  }

  @Override
  public void start()
  {
    log.debug("Starting OpenTelemetryEmitter");
  }

  @Override
  public void emit(Event e)
  {
    if (!(e instanceof ServiceMetricEvent)) {
      return;
    }
    ServiceMetricEvent event = (ServiceMetricEvent) e;

    // We generate spans for the following types of events:
    // query/time
    if (!event.getMetric().equals("query/time")) {
      return;
    }

    emitQueryTimeEvent(event);
  }

  private void emitQueryTimeEvent(ServiceMetricEvent event)
  {
    Context context = propagator.extract(Context.current(), getContextAsString(event), DRUID_CONTEXT_MAP_GETTER);

    try (Scope scope = context.makeCurrent()) {
      DateTime endTime = event.getCreatedTime();
      DateTime startTime = endTime.minusMillis(event.getValue().intValue());

      tracer.spanBuilder(event.getService())
            .setStartTimestamp(startTime.getMillis(), TimeUnit.MILLISECONDS)
            .startSpan()
            .end(endTime.getMillis(), TimeUnit.MILLISECONDS);
    }
  }

  private static Map<String, String> getContextAsString(ServiceMetricEvent event)
  {
    return getContext(event).entrySet()
                            .stream()
                            .filter(entry -> entry.getValue() != null)
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }

  private static Map<String, Object> getContext(ServiceMetricEvent event)
  {
    Object context = event.getUserDims().get("context");
    if (!(context instanceof Map)) {
      return Collections.emptyMap();
    }
    return (Map<String, Object>) context;
  }

  @Override
  public void flush() throws IOException
  {
  }

  @Override
  public void close() throws IOException
  {
  }
}

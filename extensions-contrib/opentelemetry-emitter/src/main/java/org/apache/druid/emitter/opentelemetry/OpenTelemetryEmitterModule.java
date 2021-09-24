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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.autoconfigure.OpenTelemetrySdkAutoConfiguration;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;

import java.util.Collections;
import java.util.List;

public class OpenTelemetryEmitterModule implements DruidModule
{
  private static final Logger log = new Logger(OpenTelemetryEmitterModule.class);
  private static final String EMITTER_TYPE = "opentelemetry";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter." + EMITTER_TYPE, OpenTelemetryEmitterConfig.class);
  }

  @Provides
  @ManageLifecycle
  @Named(EMITTER_TYPE)
  public Emitter getEmitter(OpenTelemetryEmitterConfig config, ObjectMapper mapper)
  {
    OpenTelemetrySdkAutoConfiguration.initialize();
    String protocol = config.getProtocol();
    String endpoint = config.getEndpoint();
    String exporter = config.getExporter();
    if (protocol != null) {
      System.setProperty("otel.experimental.exporter.otlp.traces.protocol", protocol);
    }
    if (endpoint != null) {
      System.setProperty("otel.exporter.otlp.endpoint", endpoint);
    }
    if (exporter != null) {
      System.setProperty("otel.traces.exporter", exporter);
    }
    System.setProperty("otel.service.name", "org.apache.druid");
    log.info("Init OTel with otel.experimental.exporter.otlp.traces.protocol = " + protocol);
    log.info("Init OTel with otel.exporter.otlp.endpoint = " + endpoint);
    log.info("Init OTel with otel.traces.exporter = " + exporter);
    return new OpenTelemetryEmitter(GlobalOpenTelemetry.getTracer("druid-opentelemtry-extension"));
  }
}

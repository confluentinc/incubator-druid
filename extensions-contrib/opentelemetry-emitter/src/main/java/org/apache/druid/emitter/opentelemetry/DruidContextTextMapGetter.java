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

import io.opentelemetry.context.propagation.TextMapGetter;

import java.util.Map;

/**
 * Implementation of a text-based approach to read the W3C Trace Context from request.
 * Opentelemetry context propagation - https://opentelemetry.io/docs/java/manual_instrumentation/#context-propagation
 * W3C Trace Context - https://www.w3.org/TR/trace-context/
 */
public class DruidContextTextMapGetter implements TextMapGetter<Map<String, String>>
{
  @Override
  public String get(Map<String, String> carrier, String key)
  {
    if (carrier.containsKey(key)) {
      return carrier.get(key);
    }
    return null;
  }

  @Override
  public Iterable<String> keys(Map<String, String> carrier)
  {
    return carrier.keySet();
  }
}

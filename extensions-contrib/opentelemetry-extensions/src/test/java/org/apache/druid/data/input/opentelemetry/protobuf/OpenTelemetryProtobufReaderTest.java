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

package org.apache.druid.data.input.opentelemetry.protobuf;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import org.junit.Test;

import static org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryProtobufReader.parseAnyValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class OpenTelemetryProtobufReaderTest
{

  @Test
  public void testParseAnyValue()
  {
    AnyValue.Builder anyValBuilder = AnyValue.newBuilder();
    assertEquals(100L, parseAnyValue(anyValBuilder.setIntValue(100L).build()));
    assertEquals(false, parseAnyValue(anyValBuilder.setBoolValue(false).build()));
    assertEquals("String", parseAnyValue(anyValBuilder.setStringValue("String").build()));
    assertEquals(100.0, parseAnyValue(anyValBuilder.setDoubleValue(100.0).build()));
    assertNull(parseAnyValue(anyValBuilder.setArrayValue(ArrayValue.getDefaultInstance()).build()));
  }
}

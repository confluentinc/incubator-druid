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

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryInputFormat.getSettableEntity;
import static org.junit.Assert.assertEquals;

public class OpenTelemetryProtobufInputFormatTest
{

  @Test
  public void testGetSettableByteEntity()
  {
    byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
    InputEntity ie = new ByteEntity(bytes);
    assertEquals(ie, getSettableEntity(ie).getEntity());

    SettableByteEntity<ByteEntity> se = new SettableByteEntity<>();
    se.setEntity(new ByteEntity(bytes));
    assertEquals(se, getSettableEntity(se));
  }
}

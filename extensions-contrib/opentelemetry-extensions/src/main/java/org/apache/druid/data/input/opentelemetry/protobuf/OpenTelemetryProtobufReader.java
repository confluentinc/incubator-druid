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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.seekablestream.SettableByteEntity;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public abstract class OpenTelemetryProtobufReader implements InputEntityReader
{
  protected SettableByteEntity<? extends ByteEntity> source;

  public OpenTelemetryProtobufReader(SettableByteEntity<? extends ByteEntity> source)
  {
    this.source = source;
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    Supplier<Iterator<InputRow>> supplier = Suppliers.memoize(() -> readAsList().iterator());
    return CloseableIterators.withEmptyBaggage(new Iterator<InputRow>() {
      @Override
      public boolean hasNext()
      {
        return supplier.get().hasNext();
      }
      @Override
      public InputRow next()
      {
        return supplier.get().next();
      }
    });
  }

  private List<InputRow> readAsList()
  {
    try {
      ByteBuffer buffer = source.getEntity().getBuffer();
      List<InputRow> rows = getRows(buffer);
      // Explicitly move the position assuming that all the remaining bytes have been consumed because the protobuf
      // parser does not update the position itself
      buffer.position(buffer.limit());
      return rows;
    }
    catch (InvalidProtocolBufferException e) {
      throw new ParseException(null, e, "Protobuf message could not be parsed");
    }
  }

  public abstract List<InputRow> getRows(ByteBuffer byteBuffer) throws InvalidProtocolBufferException;

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample()
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}

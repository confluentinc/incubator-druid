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

package org.apache.druid.data.input.kafkainput;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class KafkaInputReader implements InputEntityReader
{
  private final DimensionsSpec dimensionsSpec;
  private final KafkaHeaderReader headerParser;
  private final InputEntityReader keyParser;
  private final InputEntityReader payloadParser;
  private final String keyLabelPrefix;

  public KafkaInputReader(
      DimensionsSpec dimensionsSpec,
      KafkaHeaderReader headerParser,
      InputEntityReader keyParser,
      InputEntityReader payloadParser,
      String keyLabelPrefix
  )
  {
    this.dimensionsSpec = dimensionsSpec;
    this.headerParser = headerParser;
    this.keyParser = keyParser;
    this.payloadParser = payloadParser;
    this.keyLabelPrefix = keyLabelPrefix;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    MapBasedInputRow keyRow = null;
    CloseableIterator<InputRow> keyIterator = null;
    MapBasedInputRow headerRow = (MapBasedInputRow) this.headerParser.read();

    if (this.keyParser != null) {
      keyIterator = this.keyParser.read();
      // Key currently only takes the first row and ignores the rest.
      keyRow = keyIterator.hasNext() ? (MapBasedInputRow) keyIterator.next() : null;
    }

    CloseableIterator<InputRow> iterator = this.payloadParser.read();
    List<InputRow> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      /* Currently we prefer payload attributes if there is a collision in names.
          We can change this beahvior in later changes with a config knob. This default
          behavior lets easy porting of existing inputFormats to the new one without any changes.
       */
      MapBasedInputRow row = (MapBasedInputRow) iterator.next();
      Map<String, Object> event = new HashMap<>(row.getEvent());
      final Map<String, Object> map2 = headerRow.getEvent();
      map2.forEach((key, value) -> event.merge(key, value, (v1, v2) -> v1));

      HashSet<String> newDimensions = new HashSet<String>(row.getDimensions());
      newDimensions.addAll(headerRow.getDimensions());

      // Check if kafka key name is not present in the final list
      if (keyRow != null && event.get(this.keyLabelPrefix + "key") == null) {
        event.put(
                this.keyLabelPrefix + "key",
                keyRow.getEvent().entrySet().stream().findFirst().get().getValue()
        );
        newDimensions.add(this.keyLabelPrefix + "key");
      }

      final List<String> schemaDimensions = dimensionsSpec.getDimensionNames();
      final List<String> dimensions;
      if (!schemaDimensions.isEmpty()) {
        dimensions = schemaDimensions;
      } else {
        dimensions = Lists.newArrayList(
                Sets.difference(newDimensions, dimensionsSpec.getDimensionExclusions())
        );
      }
      rows.add(new MapBasedInputRow(row.getTimestamp(), dimensions, event));
    }
    // Free the old row iterators
    if (keyIterator != null) {
      keyIterator.close();
    }
    iterator.close();
    return CloseableIterators.withEmptyBaggage(rows.iterator());
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}


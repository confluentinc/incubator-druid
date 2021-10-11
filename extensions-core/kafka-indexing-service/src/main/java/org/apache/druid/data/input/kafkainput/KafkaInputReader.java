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
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class KafkaInputReader implements InputEntityReader
{
  private static final Logger log = new Logger(KafkaInputReader.class);
  private static final String DEFAULT_KEY_STRING = "key";
  private static final String DEFAULT_TIMESTAMP_STRING = "timestamp";

  private final InputRowSchema inputRowSchema;
  private final KafkaRecordEntity record;
  private final KafkaHeaderReader headerParser;
  private final InputEntityReader keyParser;
  private final InputEntityReader valueParser;
  private final String keyLabelPrefix;
  private final String recordTimestampLabelPrefix;

  public KafkaInputReader(
      InputRowSchema inputRowSchema,
      KafkaRecordEntity record,
      KafkaHeaderReader headerParser,
      InputEntityReader keyParser,
      InputEntityReader valueParser,
      String keyLabelPrefix,
      String recordTimestampLabelPrefix
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.record = record;
    this.headerParser = headerParser; //Header parser can be null by config
    this.keyParser = keyParser; //Key parser can be null by config and data
    this.valueParser = valueParser; //value parser can be null by data (tombstone records)
    this.keyLabelPrefix = keyLabelPrefix;
    this.recordTimestampLabelPrefix = recordTimestampLabelPrefix;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    // Add kafka record timestamp to the mergelist
    List<Pair<String, Object>> mergeList = new ArrayList<>();
    if (headerParser != null) {
      List<Pair<String, Object>> headerList = headerParser.read();
      mergeList.addAll(headerList);
    }

    Pair ts = new Pair(recordTimestampLabelPrefix + DEFAULT_TIMESTAMP_STRING,
                      record.getRecord().timestamp());
    if (!mergeList.contains(ts)) {
      mergeList.add(ts);
    }

    if (keyParser != null) {
      try (CloseableIterator<InputRow> keyIterator = keyParser.read()) {
        // Key currently only takes the first row and ignores the rest.
        if (keyIterator.hasNext()) {
          // Return type for the key parser should be of type MapBasedInputRow
          // Parsers returning other types are not compatible currently.
          MapBasedInputRow keyRow = (MapBasedInputRow) keyIterator.next();
          Pair key = new Pair(keyLabelPrefix + DEFAULT_KEY_STRING,
                            keyRow.getEvent().entrySet().stream().findFirst().get().getValue());
          if (!mergeList.contains(key)) {
            mergeList.add(key);
          }
        }
      }
      catch (IOException e) {
        throw e;
      }
      catch (ClassCastException e) {
        log.error(e, "Encountered ClassCastException exception, please use parsers that return MapBasedInputRow based rows.");
      }
    }

    if (valueParser != null) {
      CloseableIterator<InputRow> iterator = valueParser.read();

      return new CloseableIterator<InputRow>()
      {
        @Override
        public boolean hasNext()
        {
          return iterator.hasNext();
        }

        @Override
        public InputRow next()
        {
          InputRow row = null;
          if (!iterator.hasNext()) {
            throw new NoSuchElementException();
          }
          try {
            // Return type for the value parser should be of type MapBasedInputRow
            // Parsers returning other types are not compatible currently.
            MapBasedInputRow valueRow = (MapBasedInputRow) iterator.next();
            Map<String, Object> event = new HashMap<>();
            HashSet<String> newDimensions = new HashSet<String>(valueRow.getDimensions());
            for (Pair<String, Object> ele : mergeList) {
              event.put(ele.lhs, ele.rhs);
              newDimensions.add(ele.lhs);
            }
            /* Currently we prefer payload attributes if there is a collision in names.
                We can change this beahvior in later changes with a config knob. This default
                behavior lets easy porting of existing inputFormats to the new one without any changes.
              */
            event.putAll(valueRow.getEvent());

            // Remove the dummy timestamp added in KafkaInputFormat
            newDimensions.remove("__kif_auto_timestamp");

            final List<String> schemaDimensions = inputRowSchema.getDimensionsSpec().getDimensionNames();
            final List<String> dimensions;
            if (!schemaDimensions.isEmpty()) {
              dimensions = schemaDimensions;
            } else {
              dimensions = Lists.newArrayList(
                  Sets.difference(newDimensions, inputRowSchema.getDimensionsSpec().getDimensionExclusions())
              );
            }
            row = new MapBasedInputRow(
                inputRowSchema.getTimestampSpec().extractTimestamp(event),
                dimensions,
                event
            );
          }
          catch (ClassCastException e) {
            log.error(e, "Encountered ClassCastException exception, please use parsers that return MapBasedInputRow based rows.");
          }
          return row;
        }

        @Override
        public void close() throws IOException
        {
          iterator.close();
        }
      };
    } else {
      InputRow row = null;
      Map<String, Object> event = new HashMap<>();
      for (Pair<String, Object> ele : mergeList) {
        event.put(ele.lhs, ele.rhs);
      }
      HashSet<String> newDimensions = new HashSet<String>(event.keySet());
      final List<String> schemaDimensions = inputRowSchema.getDimensionsSpec().getDimensionNames();
      final List<String> dimensions;
      if (!schemaDimensions.isEmpty()) {
        dimensions = schemaDimensions;
      } else {
        dimensions = Lists.newArrayList(
            Sets.difference(newDimensions, inputRowSchema.getDimensionsSpec().getDimensionExclusions())
        );
      }
      row = new MapBasedInputRow(
          inputRowSchema.getTimestampSpec().extractTimestamp(event),
          dimensions,
          event
      );
      List<InputRow> rows = Collections.singletonList(row);
      return CloseableIterators.withEmptyBaggage(rows.iterator());
    }
  }

  // This API is not implemented yet!
  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}

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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
    this.headerParser = headerParser;
    this.keyParser = keyParser;
    this.valueParser = valueParser;
    this.keyLabelPrefix = keyLabelPrefix;
    this.recordTimestampLabelPrefix = recordTimestampLabelPrefix;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    Map<String, Object> mergeList = new HashMap<>(headerParser.read());
    // Add kafka record timestamp to the mergelist
    mergeList.put(recordTimestampLabelPrefix + DEFAULT_TIMESTAMP_STRING, record.getRecord().timestamp());

    // Return type for the key parser should be of type MapBasedInputRow
    // Parsers returning other types are not compatible currently.
    if (keyParser != null) {
      try (CloseableIterator<InputRow> keyIterator = keyParser.read()) {
        // Key currently only takes the first row and ignores the rest.
        if (keyIterator.hasNext()) {
          MapBasedInputRow keyRow = (MapBasedInputRow) keyIterator.next();
          mergeList.put(
              keyLabelPrefix + DEFAULT_KEY_STRING,
              keyRow.getEvent().entrySet().stream().findFirst().get().getValue()
          );
        }
      }
      catch (Exception e) {
        if (e instanceof IOException) {
          log.error(e, "Encountered IOException during key parsing.");
          throw (IOException) e;
        } else if (e instanceof ParseException) {
          log.error(e, "Encountered key parsing exception.");
        } else {
          log.error(e, "Encountered exception during key parsing.");
          throw e;
        }
      }
    }

    List<InputRow> rows = new ArrayList<>();

    // Return type for the value parser should be of type MapBasedInputRow
    // Parsers returning other types are not compatible currently.
    if (valueParser != null) {
      try (CloseableIterator<InputRow> iterator = valueParser.read()) {
        while (iterator.hasNext()) {
      /* Currently we prefer payload attributes if there is a collision in names.
          We can change this beahvior in later changes with a config knob. This default
          behavior lets easy porting of existing inputFormats to the new one without any changes.
       */
          MapBasedInputRow row = (MapBasedInputRow) iterator.next();
          Map<String, Object> event = new HashMap<>(mergeList);
          event.putAll(row.getEvent());

          HashSet<String> newDimensions = new HashSet<String>(row.getDimensions());
          newDimensions.addAll(mergeList.keySet());
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
          rows.add(new MapBasedInputRow(
              inputRowSchema.getTimestampSpec().extractTimestamp(event),
              dimensions,
              event
          ));
        }
      }
      catch (Exception e) {
        if (e instanceof IOException) {
          log.error(e, "Encountered IOException during value parsing.");
          throw (IOException) e;
        } else if (e instanceof ParseException) {
          log.error(e, "Encountered value parsing exception.");
        } else {
          log.error(e, "Encountered exception during value parsing.");
          throw e;
        }
      }
    } else {
      HashSet<String> newDimensions = new HashSet<String>(mergeList.keySet());
      final List<String> schemaDimensions = inputRowSchema.getDimensionsSpec().getDimensionNames();
      final List<String> dimensions;
      if (!schemaDimensions.isEmpty()) {
        dimensions = schemaDimensions;
      } else {
        dimensions = Lists.newArrayList(
            Sets.difference(newDimensions, inputRowSchema.getDimensionsSpec().getDimensionExclusions())
        );
      }
      rows.add(new MapBasedInputRow(
          inputRowSchema.getTimestampSpec().extractTimestamp(mergeList),
          dimensions,
          mergeList
      ));
    }

    return CloseableIterators.withEmptyBaggage(rows.iterator());
  }

  // This API is not implemented yet!
  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}

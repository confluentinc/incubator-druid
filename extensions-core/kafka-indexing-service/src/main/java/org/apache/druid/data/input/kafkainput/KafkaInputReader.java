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

import org.apache.druid.java.util.common.logger.Logger;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class KafkaInputReader implements InputEntityReader
{
  private static final Logger log = new Logger(KafkaInputReader.class);
  private final InputRowSchema inputRowSchema;
  private final KafkaRecordEntity record;
  private final KafkaHeaderReader headerParser;
  private final InputEntityReader keyParser;
  private final InputEntityReader payloadParser;
  private final String keyLabelPrefix;
  private final String recordTimestampLabelPrefix;

  public KafkaInputReader(
      InputRowSchema inputRowSchema,
      KafkaRecordEntity record,
      KafkaHeaderReader headerParser,
      InputEntityReader keyParser,
      InputEntityReader payloadParser,
      String keyLabelPrefix,
      String recordTimestampLabelPrefix
  )
  {
    this.inputRowSchema = inputRowSchema;
    this.record = record;
    this.headerParser = headerParser;
    this.keyParser = keyParser;
    this.payloadParser = payloadParser;
    this.keyLabelPrefix = keyLabelPrefix;
    this.recordTimestampLabelPrefix = recordTimestampLabelPrefix;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    Map<String, Object> mergeList = new HashMap<>(this.headerParser.read());

    if (this.keyParser != null) {
      CloseableIterator<InputRow> keyIterator = this.keyParser.read();
      // Key currently only takes the first row and ignores the rest.
      if (keyIterator.hasNext()) {
        MapBasedInputRow keyRow =  (MapBasedInputRow) keyIterator.next();
        mergeList.put(
            this.keyLabelPrefix + "key",
            keyRow.getEvent().entrySet().stream().findFirst().get().getValue()
        );
      }
      keyIterator.close();
    }
    // Add kafka record timestamp to the mergelist
    mergeList.put(this.recordTimestampLabelPrefix + "timestamp", this.record.getRecord().timestamp());
    //log.warn("LOKI Kafka record :" + new String(this.record.getRecord().value(), StandardCharsets.UTF_8));

    CloseableIterator<InputRow> iterator = this.payloadParser.read();
    List<InputRow> rows = new ArrayList<>();
    try {
      while (iterator.hasNext()) {
      /* Currently we prefer payload attributes if there is a collision in names.
          We can change this beahvior in later changes with a config knob. This default
          behavior lets easy porting of existing inputFormats to the new one without any changes.
       */
        MapBasedInputRow row = (MapBasedInputRow) iterator.next();
        Map<String, Object> event = new HashMap<>(row.getEvent());
        mergeList.forEach((key, value) -> event.merge(key, value, (v1, v2) -> v1));

        HashSet<String> newDimensions = new HashSet<String>(row.getDimensions());
        newDimensions.addAll(mergeList.keySet());
        // Free mergeList
        mergeList = null;
        // Remove the dummy timestamp added in KafkaInputFormat
        newDimensions.remove("__kif_auto_timestamp");

        final List<String> schemaDimensions = this.inputRowSchema.getDimensionsSpec().getDimensionNames();
        final List<String> dimensions;
        if (!schemaDimensions.isEmpty()) {
          dimensions = schemaDimensions;
        } else {
          dimensions = Lists.newArrayList(
              Sets.difference(newDimensions, this.inputRowSchema.getDimensionsSpec().getDimensionExclusions())
          );
        }
        rows.add(new MapBasedInputRow(
            this.inputRowSchema.getTimestampSpec().extractTimestamp(event),
            dimensions,
            event));
      }
    } catch (Exception e) {
      log.error(e, "Encountered exception.");
      if( e instanceof IOException ) {
        throw (IOException)e;
      }
    } finally {
      // Free the old row iterators
        iterator.close();
    }
    return CloseableIterators.withEmptyBaggage(rows.iterator());
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}


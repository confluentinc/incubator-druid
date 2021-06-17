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

package org.apache.druid.data.kafkainput;

import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.*;

public class KafkaHeaderReader implements InputEntityReader
{
  private static final Logger log = new Logger(KafkaHeaderReader.class);
  private static final String SEPARATOR = "-";

  private final KafkaRecordEntity source;
  private final String headerLabelPrefix;

  public KafkaHeaderReader(
      KafkaRecordEntity source,
      String headerLabelPrefix
  )
  {
    this.source = source;
    this.headerLabelPrefix = headerLabelPrefix;
  }

  @Override
  public CloseableIterator<InputRow> read()
  {
    return CloseableIterators.withEmptyBaggage(parseHeaders(this.source).iterator());
  }

  private List<InputRow> parseHeaders(KafkaRecordEntity data)
  {
    List<InputRow> rows = new ArrayList<>();
    ConsumerRecord<byte[], byte[]> record = data.getRecord();
    log.warn("Kafka record:", record.toString());
    return rows;
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample()
  {
    return read().map(row -> InputRowListPlusRawValues.of(row, ((MapBasedInputRow) row).getEvent()));
  }
}

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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 */
public class SegmentsMetadataPublisher
{
  private static final Logger log = new Logger(SegmentsMetadataPublisher.class);
  private static final Random RANDOM = ThreadLocalRandom.current();
  private final KafkaProducer<byte[], byte[]> producer;

  public SegmentsMetadataPublisher()
  {
    final Map<String, Object> props = new HashMap<>();
    //API key --> aggregation_poc_svc
    final String username = "";
    final String password = "";
    props.put("bootstrap.servers", "pkc-2099m.us-west-2.aws.devel.cpdev.cloud:9092"); // Hack for now to sandbox cluster
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
    props.put("key.serializer", ByteArraySerializer.class.getName());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    props.put("acks", "all");
    props.put("enable.idempotence", "true");
    this.producer = new KafkaProducer<>(props);
  }

  private static byte[] jb(String datasource, String createdDate, String start, String end, String version)
  {
    try {
      return new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
                      .put("datasource", datasource)
                      .put("createdDate", createdDate)
                      .put("start", start)
                      .put("end", end)
                      .put("version", version)
                      .build()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private KafkaProducer<byte[], byte[]> getSegmentEmitter()
  {
   return this.producer;
  }

  public void publishSegmentMetadata(String datasource, String createdDate, String start, String end, String version)
  {
    log.error("Sending segment publish events to kafka[test_topic]: Datasource:[%s] createDate:[%s] start:[%s] end:[%s] version:[%s]", datasource, createdDate, start, end, version);
    KafkaProducer<byte[], byte[]> producer = getSegmentEmitter();
    try {
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(
          "aggregation_poc_test_topic",
          datasource.getBytes(StandardCharsets.UTF_8),
          jb(datasource, createdDate, start, end, version)
      );
      producer.send(record);
    } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
      // We can't recover from these exceptions, so our only option is to close the producer and exit.
      producer.close();
      log.error("Error occurred. Error: %s", e.getMessage());
    } catch (KafkaException e) {
      // For all other exceptions, just abort the transaction and try again.
      log.error("Error occurred. Error: %s", e.getMessage());
    }
  }
}

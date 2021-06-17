package org.apache.druid.data.input.kafkainput;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.*;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class KafkaStringHeaderFormatTest {
    private KafkaRecordEntity inputEntity;
    private long timestamp = DateTimes.of("2021-06-24T00:00:00.000Z").getMillis();
    private static final Iterable<Header> SAMPLE_HEADERS = ImmutableList.of(new Header() {
        @Override
        public String key() { return "encoding"; }
        @Override
        public byte[] value() { return "application/json".getBytes(StandardCharsets.UTF_8); }
    },
        new Header() {
            @Override
            public String key() { return "kafkapkc"; }
            @Override
            public byte[] value() { return "pkc-bar".getBytes(StandardCharsets.UTF_8); }
        });

    @Test
    public void testDefaultHeaderFormat() throws IOException {
        String headerLabelPrefix = "test.kafka.header.";
        String timestampLablePrefix = "test.kafka.newts.";
        Headers headers = new RecordHeaders(SAMPLE_HEADERS);
        inputEntity = new KafkaRecordEntity(new ConsumerRecord<byte[], byte[]>(
                "sample", 0, 0, timestamp,
                null, null, 0, 0,
                null, "sampleValue".getBytes(StandardCharsets.UTF_8), headers));
        InputRow expectedResults = new MapBasedInputRow(
                timestamp,
                ImmutableList.of(
                        "test.kafka.header.encoding",
                        "test.kafka.header.kafkapkc",
                        "test.kafka.newts.timestamp"
                ),
                ImmutableMap.of(
                        "test.kafka.header.encoding",
                        "application/json",
                        "test.kafka.header.kafkapkc",
                        "pkc-bar",
                        "test.kafka.newts.timestamp",
                        DateTimes.of("2021-06-24T00:00:00.000Z").getMillis()
                )
        );

        KafkaHeaderFormat headerInput = new KafkaStringHeaderFormat(null);
        KafkaHeaderReader headerParser = headerInput.createReader(inputEntity.getRecord().headers(),
                                                                    inputEntity.getRecord().timestamp(),
                                                                    headerLabelPrefix,
                                                                    timestampLablePrefix);
        Assert.assertEquals(expectedResults, headerParser.read());
    }

    @Test
    public void testASCIIHeaderFormat() throws IOException {
        Iterable<Header> HEADERS = ImmutableList.of(
                new Header() {
                    @Override
                    public String key() { return "encoding"; }
                    @Override
                    public byte[] value() { return "application/json".getBytes(StandardCharsets.US_ASCII); }
                },
                new Header() {
                    @Override
                    public String key() { return "kafkapkc"; }
                    @Override
                    public byte[] value() { return "pkc-bar".getBytes(StandardCharsets.US_ASCII); }
        });

        String headerLabelPrefix = "test.kafka.header.";
        String timestampLablePrefix = "test.kafka.newts.";
        Headers headers = new RecordHeaders(HEADERS);
        inputEntity = new KafkaRecordEntity(new ConsumerRecord<byte[], byte[]>(
                "sample", 0, 0, timestamp,
                null, null, 0, 0,
                null, "sampleValue".getBytes(StandardCharsets.UTF_8), headers));
        InputRow expectedResults = new MapBasedInputRow(
                timestamp,
                ImmutableList.of(
                        "test.kafka.header.encoding",
                        "test.kafka.header.kafkapkc",
                        "test.kafka.newts.timestamp"
                ),
                ImmutableMap.of(
                        "test.kafka.header.encoding",
                        "application/json",
                        "test.kafka.header.kafkapkc",
                        "pkc-bar",
                        "test.kafka.newts.timestamp",
                        DateTimes.of("2021-06-24T00:00:00.000Z").getMillis()
                )
        );

        KafkaHeaderFormat headerInput = new KafkaStringHeaderFormat("US-ASCII");
        KafkaHeaderReader headerParser = headerInput.createReader(inputEntity.getRecord().headers(),
                inputEntity.getRecord().timestamp(),
                headerLabelPrefix,
                timestampLablePrefix);

        MapBasedInputRow row = (MapBasedInputRow)headerParser.read();
        Assert.assertEquals(expectedResults, headerParser.read());
    }
}
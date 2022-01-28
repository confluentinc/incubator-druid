package org.apache.druid.metadata;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class SegmentsMetadataPublisherTest
{
  @Test
  public void testProduce10Records() throws InterruptedException
  {
    SegmentsMetadataPublisher segmentProducer = new SegmentsMetadataPublisher();
    for (int i=1; i<=10; i++)
    {
      TimeUnit.SECONDS.sleep(1);
      segmentProducer.publishSegmentMetadata("testDS", DateTimes.nowUtc().toString(), DateTimes.nowUtc().toString(), DateTimes.nowUtc().toString(), "version");
    }
  }
}
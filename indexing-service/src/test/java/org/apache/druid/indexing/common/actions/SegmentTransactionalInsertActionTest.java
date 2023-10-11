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

package org.apache.druid.indexing.common.actions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.LockResult;
import org.apache.druid.indexing.overlord.ObjectMetadata;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TimeChunkLockRequest;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.assertj.core.api.Assertions;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public class SegmentTransactionalInsertActionTest
{
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TaskActionTestKit actionTestKit = new TaskActionTestKit();

  private static final String DATA_SOURCE = "none";
  private static final Interval INTERVAL = Intervals.of("2020/2020T01");
  private static final String PARTY_YEAR = "1999";
  private static final String THE_DISTANT_FUTURE = "3000";

  private static final DataSegment SEGMENT1 = new DataSegment(
      DATA_SOURCE,
      INTERVAL,
      PARTY_YEAR,
      ImmutableMap.of(),
      ImmutableList.of(),
      ImmutableList.of(),
      new LinearShardSpec(0),
      9,
      1024
  );

  private static final DataSegment SEGMENT2 = new DataSegment(
      DATA_SOURCE,
      INTERVAL,
      PARTY_YEAR,
      ImmutableMap.of(),
      ImmutableList.of(),
      ImmutableList.of(),
      new LinearShardSpec(1),
      9,
      1024
  );

  private static final DataSegment SEGMENT3 = new DataSegment(
      DATA_SOURCE,
      INTERVAL,
      THE_DISTANT_FUTURE,
      ImmutableMap.of(),
      ImmutableList.of(),
      ImmutableList.of(),
      new LinearShardSpec(1),
      9,
      1024
  );

  private LockResult acquireTimeChunkLock(TaskLockType lockType, Task task, Interval interval, long timeoutMs)
      throws InterruptedException
  {
    return actionTestKit.getTaskLockbox().lock(task, new TimeChunkLockRequest(lockType, task, interval, null), timeoutMs);
  }

  @Test
  public void testTransactionalUpdateDataSourceMetadata() throws Exception
  {
    final Task task = NoopTask.create();
    actionTestKit.getTaskLockbox().add(task);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    SegmentPublishResult result1 = SegmentTransactionalInsertAction.appendAction(
        ImmutableSet.of(SEGMENT1),
        new ObjectMetadata(null),
        new ObjectMetadata(ImmutableList.of(1))
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(SEGMENT1)), result1);

    SegmentPublishResult result2 = SegmentTransactionalInsertAction.appendAction(
        ImmutableSet.of(SEGMENT2),
        new ObjectMetadata(ImmutableList.of(1)),
        new ObjectMetadata(ImmutableList.of(2))
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(SEGMENT2)), result2);

    Assertions.assertThat(
        actionTestKit.getMetadataStorageCoordinator()
                     .retrieveUsedSegmentsForInterval(DATA_SOURCE, INTERVAL, Segments.ONLY_VISIBLE)
    ).containsExactlyInAnyOrder(SEGMENT1, SEGMENT2);

    Assert.assertEquals(
        new ObjectMetadata(ImmutableList.of(2)),
        actionTestKit.getMetadataStorageCoordinator().retrieveDataSourceMetadata(DATA_SOURCE)
    );
  }

  @Test
  public void testTransactionalUpdateDataSourceMetadataWithRecoverFromMetadataMismatch() throws Exception
  {
    final Task task1 = NoopTask.create();
    final TaskLockbox taskLockbox1 = new TaskLockbox(actionTestKit.getTaskStorage(), actionTestKit.getMetadataStorageCoordinator());
    taskLockbox1.add(task1);
    taskLockbox1.lock(task1, new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task1, INTERVAL, null), 5000);

    final Task task2 = NoopTask.create();
    final TaskLockbox taskLockbox2 = new TaskLockbox(actionTestKit.getTaskStorage(), actionTestKit.getMetadataStorageCoordinator());
    taskLockbox2.add(task2);
    taskLockbox2.lock(task2, new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task2, INTERVAL, null), 5000);

    final TaskLockConfig taskLockConfig = new TaskLockConfig()
    {
      @Override
      public boolean isBatchSegmentAllocation()
      {
        return true;
      }

      @Override
      public long getBatchAllocationWaitTime()
      {
        return 10L;
      }
    };
    TaskActionToolbox taskActionToolbox = actionTestKit.getTaskActionToolbox();

    // Task1 and Task2 tries to publish segment1 and segment2 for same partition at around same time.
    // With different start and end offsets. Segment2 -> {1 - 2}, Segment1 -> {null - 1}
    Future<SegmentPublishResult> result2Future = CompletableFuture.supplyAsync(() -> {
      return SegmentTransactionalInsertAction.appendAction(
              ImmutableSet.of(SEGMENT2),
              new ObjectMetadata(ImmutableList.of(1)),
              new ObjectMetadata(ImmutableList.of(2))
      ).perform(
              task2,
              new TaskActionToolbox(
                      taskLockbox2,
                      taskActionToolbox.getTaskStorage(),
                      taskActionToolbox.getIndexerMetadataStorageCoordinator(),
                      new SegmentAllocationQueue(
                              taskLockbox2,
                              taskLockConfig,
                              taskActionToolbox.getIndexerMetadataStorageCoordinator(),
                              taskActionToolbox.getEmitter(),
                              ScheduledExecutors::fixed
                      ),
                      taskActionToolbox.getEmitter(),
                      EasyMock.createMock(SupervisorManager.class),
                      taskActionToolbox.getJsonMapper()
              )
      );
    });


    Future<SegmentPublishResult> result1Future = CompletableFuture.supplyAsync(() -> {
      return SegmentTransactionalInsertAction.appendAction(
              ImmutableSet.of(SEGMENT1),
              new ObjectMetadata(null),
              new ObjectMetadata(ImmutableList.of(1))
      ).perform(
              task1,
              new TaskActionToolbox(
                      taskLockbox1,
                      taskActionToolbox.getTaskStorage(),
                      taskActionToolbox.getIndexerMetadataStorageCoordinator(),
                      new SegmentAllocationQueue(
                              taskLockbox1,
                              taskLockConfig,
                              taskActionToolbox.getIndexerMetadataStorageCoordinator(),
                              taskActionToolbox.getEmitter(),
                              ScheduledExecutors::fixed
                      ),
                      taskActionToolbox.getEmitter(),
                      EasyMock.createMock(SupervisorManager.class),
                      taskActionToolbox.getJsonMapper()
              )

      );
    });

    SegmentPublishResult result2 = result2Future.get();
    SegmentPublishResult result1 = result1Future.get();

    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(SEGMENT1)), result1);
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(SEGMENT2)), result2);

    Assertions.assertThat(
            actionTestKit.getMetadataStorageCoordinator()
                    .retrieveUsedSegmentsForInterval(DATA_SOURCE, INTERVAL, Segments.ONLY_VISIBLE)
    ).containsExactlyInAnyOrder(SEGMENT1, SEGMENT2);

    Assert.assertEquals(
            new ObjectMetadata(ImmutableList.of(2)),
            actionTestKit.getMetadataStorageCoordinator().retrieveDataSourceMetadata(DATA_SOURCE)
    );
  }

  @Test
  public void testTransactionalDropSegments() throws Exception
  {
    final Task task = NoopTask.create();
    actionTestKit.getTaskLockbox().add(task);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    SegmentPublishResult result1 = SegmentTransactionalInsertAction.overwriteAction(
        null,
        null,
        ImmutableSet.of(SEGMENT1)
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(SEGMENT1)), result1);

    SegmentPublishResult result2 = SegmentTransactionalInsertAction.overwriteAction(
        null,
        ImmutableSet.of(SEGMENT1),
        ImmutableSet.of(SEGMENT2)
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(SEGMENT2)), result2);

    Assertions.assertThat(
        actionTestKit.getMetadataStorageCoordinator()
                     .retrieveUsedSegmentsForInterval(DATA_SOURCE, INTERVAL, Segments.ONLY_VISIBLE)
    ).containsExactlyInAnyOrder(SEGMENT2);
  }

  @Test
  public void testFailTransactionalUpdateDataSourceMetadata() throws Exception
  {
    final Task task = NoopTask.create();
    actionTestKit.getTaskLockbox().add(task);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    SegmentPublishResult result = SegmentTransactionalInsertAction.appendAction(
        ImmutableSet.of(SEGMENT1),
        new ObjectMetadata(ImmutableList.of(1)),
        new ObjectMetadata(ImmutableList.of(2))
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );

    Assert.assertEquals(SegmentPublishResult.fail("org.apache.druid.metadata.RetryTransactionException: Aborting transaction!"), result);
  }

  @Test
  public void testFailTransactionalDropSegment() throws Exception
  {
    final Task task = NoopTask.create();
    actionTestKit.getTaskLockbox().add(task);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    SegmentPublishResult result = SegmentTransactionalInsertAction.overwriteAction(
        null,
        // SEGMENT1 does not exist, hence will fail to drop
        ImmutableSet.of(SEGMENT1),
        ImmutableSet.of(SEGMENT2)
    ).perform(
        task,
        actionTestKit.getTaskActionToolbox()
    );

    Assert.assertEquals(SegmentPublishResult.fail("org.apache.druid.metadata.RetryTransactionException: Aborting transaction!"), result);
  }

  @Test
  public void testFailBadVersion() throws Exception
  {
    final Task task = NoopTask.create();
    final SegmentTransactionalInsertAction action = SegmentTransactionalInsertAction.overwriteAction(
        null,
        null,
        ImmutableSet.of(SEGMENT3)
    );
    actionTestKit.getTaskLockbox().add(task);
    acquireTimeChunkLock(TaskLockType.EXCLUSIVE, task, INTERVAL, 5000);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(CoreMatchers.containsString("are not covered by locks"));
    SegmentPublishResult result = action.perform(task, actionTestKit.getTaskActionToolbox());
    Assert.assertEquals(SegmentPublishResult.ok(ImmutableSet.of(SEGMENT3)), result);
  }
}

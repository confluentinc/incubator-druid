package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.http.OverlordTest;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.server.metrics.TaskSlotCountStatsMonitor;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.createMock;

public class WorkerTaskManagerMonitorTest
{
  private WorkerTaskManager workerTaskManager;
  private TaskRunner taskRunner;
  private ObjectMapper jsonMapper;
  private TaskConfig taskConfig;
  private DruidLeaderClient overlordClient;
  private Task task;
  private WorkerTaskManager.TaskDetails taskDetails;
  private TaskAnnouncement taskAnnouncement;

  @Before
  public void setUp(){
    task = createMock(Task.class);
    EasyMock.expect(task.getDataSource()).andReturn("dummy_DS1");
    EasyMock.replay(task);
    taskDetails = createMock(WorkerTaskManager.TaskDetails.class);
    EasyMock.expect(taskDetails.getDataSource()).andReturn("dummy_DS2");
    EasyMock.replay(taskDetails);
    taskAnnouncement = createMock(TaskAnnouncement.class);
    EasyMock.expect(taskAnnouncement.getTaskDataSource()).andReturn("dummy_DS3");
    EasyMock.replay(taskAnnouncement);
    taskRunner = createMock(TaskRunner.class);
    taskConfig = createMock(TaskConfig.class);
    overlordClient = createMock(DruidLeaderClient.class);
    workerTaskManager = new WorkerTaskManager(jsonMapper, taskRunner, taskConfig, overlordClient){
      @Override
      public Map<String, TaskDetails> getRunningTasks()
      {
        Map<String, TaskDetails> runningTasks = new HashMap<>();
        runningTasks.put("taskId1", taskDetails);
        return runningTasks;
      }
      @Override
      public Map<String,TaskAnnouncement> getCompletedTasks()
      {
        Map<String, TaskAnnouncement> completedTasks = new HashMap<>();
        completedTasks.put("taskId2", taskAnnouncement);
        return completedTasks;
      }
      @Override
      public Map<String, Task> getAssignedTasks()
      {
        Map<String, Task> assignedTasks = new HashMap<>();
        assignedTasks.put("taskId3", task);
        return assignedTasks;
      }
    };
  }

  @Test
  public void testWorkerTaskManagerMonitor()
  {
    final WorkerTaskManagerMonitor monitor = new WorkerTaskManagerMonitor(workerTaskManager);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    Assert.assertEquals(3, emitter.getEvents().size());
    emitter.verifyValue("worker/task/running/count", 1);
    emitter.verifyValue("worker/task/assigned/count", 1);
    emitter.verifyValue("worker/task/completed/count", 1);
  }
}

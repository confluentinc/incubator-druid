package org.apache.druid.indexing.worker;

import com.google.inject.Inject;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;
import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

public class WorkerTaskManagerMonitor extends AbstractMonitor {

    private final WorkerTaskManager workerTaskManager;

    private static final String WorkerRunningTaskCountMetric = "worker/task/running/count";
    private static final String WorkerAssignedTaskCountMetric = "worker/task/assigned/count";
    private static final String WorkerCompletedTaskCountMetric = "worker/task/completed/count";

    @Inject
    public WorkerTaskManagerMonitor(WorkerTaskManager workerTaskManager)
    {
        this.workerTaskManager = workerTaskManager;
    }

    @Override
    public boolean doMonitor(ServiceEmitter emitter) {
        final Map<String, Integer> runningTasks, assignedTasks, completedTasks;
        runningTasks = getRunningDatasourceTasks(workerTaskManager.getRunningTasks());
        assignedTasks = getAssignedDataSourceTasks(workerTaskManager.getAssignedTasks());
        completedTasks = getCompletedDataSourceTaskMap(workerTaskManager.getCompletedTasks());

        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
        emitWorkerTaskMetric(builder, emitter, WorkerRunningTaskCountMetric, runningTasks);
        emitWorkerTaskMetric(builder, emitter, WorkerAssignedTaskCountMetric, assignedTasks);
        emitWorkerTaskMetric(builder, emitter, WorkerCompletedTaskCountMetric, completedTasks);
        return true;
    }

    public void emitWorkerTaskMetric(ServiceMetricEvent.Builder builder, ServiceEmitter emitter, String metricName, Map<String, Integer> dataSourceTaskMap){
        for (Map.Entry<String, Integer> dataSourceTaskCount : dataSourceTaskMap.entrySet()) {
            builder.setDimension(DruidMetrics.DATASOURCE, dataSourceTaskCount.getKey());
            emitter.emit(builder.build(metricName, dataSourceTaskCount.getValue()));
        }
    }

    @Nonnull
    private Map<String, Integer> getRunningDatasourceTasks(Map<String, WorkerTaskManager.TaskDetails> taskMap) {
        String dataSource;
        final Map<String, Integer> dataSourceTaskMap = new HashMap<>();

        for (Map.Entry<String, WorkerTaskManager.TaskDetails> task : taskMap.entrySet()) {
            dataSource = task.getValue().getDataSource();
            dataSourceTaskMap.putIfAbsent(dataSource, 0);
            dataSourceTaskMap.put(dataSource, dataSourceTaskMap.get(dataSource)+1);
        }
        return dataSourceTaskMap;
    }

    private Map<String, Integer> getAssignedDataSourceTasks(Map<String, Task> taskMap) {
        String dataSource;
        final Map<String, Integer> dataSourceTaskMap = new HashMap<>();

        for (Map.Entry<String, Task> task : taskMap.entrySet()) {
            dataSource = task.getValue().getDataSource();
            dataSourceTaskMap.putIfAbsent(dataSource, 0);
            dataSourceTaskMap.put(dataSource, dataSourceTaskMap.get(dataSource)+1);
        }
        return dataSourceTaskMap;
    }

    private Map<String, Integer> getCompletedDataSourceTaskMap(Map<String, TaskAnnouncement> taskMap) {
        String dataSource;
        final Map<String, Integer> dataSourceTaskMap = new HashMap<>();

        for (Map.Entry<String, TaskAnnouncement> task : taskMap.entrySet()) {
            dataSource = task.getValue().getTaskDataSource();
            dataSourceTaskMap.putIfAbsent(dataSource, 0);
            dataSourceTaskMap.put(dataSource, dataSourceTaskMap.get(dataSource)+1);
        }
        return dataSourceTaskMap;
    }
}

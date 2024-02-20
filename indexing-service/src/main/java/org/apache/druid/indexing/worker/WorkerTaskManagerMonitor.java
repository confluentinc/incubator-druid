package org.apache.druid.indexing.worker;

import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WorkerTaskManagerMonitor extends AbstractMonitor {

    private final WorkerTaskManager workerTaskManager;

    private static final String WorkerRunningTaskCountMetric = "worker/task/running/count";

    @Inject
    public WorkerTaskManagerMonitor(WorkerTaskManager workerTaskManager)
    {
        this.workerTaskManager = workerTaskManager;
    }

    @Override
    public boolean doMonitor(ServiceEmitter emitter) {
        final Map<String, Integer> dataSourceTaskMap = getDataSourceTaskMap();
        final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();

        for (Map.Entry<String, Integer> dataSourceTaskCount : dataSourceTaskMap.entrySet()) {
            builder.setDimension(DruidMetrics.DATASOURCE, dataSourceTaskCount.getKey());
            emitter.emit(builder.build(WorkerRunningTaskCountMetric, dataSourceTaskCount.getValue()));
        }
        return true;
    }

    @Nonnull
    private Map<String, Integer> getDataSourceTaskMap() {
        String dataSource;
        final Map<String, WorkerTaskManager.TaskDetails> runningTasks = workerTaskManager.getRunningTasks();
        final Map<String, Integer> dataSourceTaskMap = new HashMap<>();

        for (Map.Entry<String, WorkerTaskManager.TaskDetails> task : runningTasks.entrySet()) {
            dataSource = task.getValue().getDatasource();
            dataSourceTaskMap.putIfAbsent(dataSource, 0);
            dataSourceTaskMap.put(dataSource, dataSourceTaskMap.get(dataSource)+1);
        }
        return dataSourceTaskMap;
    }
}

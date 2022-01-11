package org.apache.druid.data.input.opentelemetry.protobuf;

import com.google.protobuf.Timestamp;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.IntDataPoint;
import io.opentelemetry.proto.metrics.v1.IntGauge;
import io.opentelemetry.proto.metrics.v1.IntSum;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.Sum;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import java.time.Instant;

public class OpenTelemetryProtobufReaderTest {

    private static final Instant INSTANT = Instant.parse("2020-01-12T09:30:01.123Z");
    private static final Timestamp TIMESTAMP = Timestamp.newBuilder()
            .setSeconds(INSTANT.getEpochSecond())
            .setNanos(INSTANT.getNano()).build();

    private final DimensionsSpec dimensionsSpec = DimensionsSpec.EMPTY;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testIntSumParse()
    {
        MetricsData request = getMetricsData(Metric.DataCase.INT_SUM);
        CloseableIterator<InputRow> rows = new OpenTelemetryProtobufReader(
                dimensionsSpec,
                new ByteEntity(request.toByteArray()),
                "metric.name",
                "descriptor.",
                "custom."
        ).read();
    }

    @Test
    public void testIntGaugeParse(){
        MetricsData request = getMetricsData(Metric.DataCase.INT_GAUGE);
        CloseableIterator<InputRow> rows = new OpenTelemetryProtobufReader(
                dimensionsSpec,
                new ByteEntity(request.toByteArray()),
                "metric.name",
                "descriptor.",
                "custom."
        ).read();
    }

    private static MetricsData getMetricsData(Metric.DataCase dataCase) {

        MetricsData.Builder requestBuilder = MetricsData.newBuilder();

        Metric.Builder metricBuilder = requestBuilder
                .addResourceMetricsBuilder()
                .addInstrumentationLibraryMetricsBuilder()
                .addMetricsBuilder();

        metricBuilder.setName("");

        IntDataPoint intDataPoint1 = IntDataPoint.newBuilder().build();
        IntDataPoint intDataPoint2 = IntDataPoint.newBuilder().build();

        NumberDataPoint numDataPoint1 = NumberDataPoint.newBuilder().build();
        NumberDataPoint numDataPoint2 = NumberDataPoint.newBuilder().build();

        switch (dataCase) {
            case INT_SUM: {
                metricBuilder.setIntSum(IntSum.newBuilder()
                        .addDataPoints(intDataPoint1)
                        .addDataPoints(intDataPoint2));
                break;

            }
            case INT_GAUGE: {
                metricBuilder.setIntGauge(IntGauge.newBuilder()
                        .addDataPoints(intDataPoint1)
                        .addDataPoints(intDataPoint2));
                break;

            }
            case SUM: {
                metricBuilder.setSum(Sum.newBuilder()
                        .addDataPoints(numDataPoint1)
                        .addDataPoints(numDataPoint2));
                break;

            }
            case GAUGE: {
                metricBuilder.setGauge(Gauge.newBuilder()
                        .addDataPoints(numDataPoint1)
                        .addDataPoints(numDataPoint2));
                break;

            }
            default:
                throw new IllegalStateException("Unexpected value: " + dataCase);
        }

        requestBuilder.getResourceMetricsBuilder(0).getResourceBuilder()
                .addAttributes(
                        KeyValue.newBuilder()
                                .setKey("env_key")
                                .setValue(
                                        AnyValue.newBuilder()
                                                .setStringValue("env_val")
                                )
                );


        return requestBuilder.build();
    }

}

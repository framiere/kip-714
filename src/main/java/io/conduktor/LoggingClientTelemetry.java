package io.conduktor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class LoggingClientTelemetry implements ClientTelemetry, MetricsReporter, ClientTelemetryReceiver {

    private static final Logger log = LoggerFactory.getLogger(LoggingClientTelemetry.class);

    public LoggingClientTelemetry() {
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        for (KafkaMetric m : metrics) {
            log.info("Initializing : " + m.metricName() + " with " + m.metricValue().toString());
        }
    }

    @Override
    public void exportMetrics(AuthorizableRequestContext context, ClientTelemetryPayload payload) {
        try {
            log.info("Context: {}", context);
        } catch (Exception e) {
            //
        }
    }

    @Override
    public void metricChange(KafkaMetric kafkaMetric) {
        log.info("Changing " + kafkaMetric.metricName() + " to " + kafkaMetric.metricValue().toString());
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        log.info("Removing: " + metric.metricName());
    }


    @Override
    public void configure(Map<String, ?> configs) {
        log.info("Configuration: {}", configs);
    }

    @Override
    public ClientTelemetryReceiver clientReceiver() {
        return this;
    }

    @Override
    public void close() {
    }

}
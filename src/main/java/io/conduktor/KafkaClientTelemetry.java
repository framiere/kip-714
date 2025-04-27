package io.conduktor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import io.opentelemetry.proto.metrics.v1.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class KafkaClientTelemetry implements ClientTelemetry, MetricsReporter, ClientTelemetryReceiver {

    private static final Logger log = LoggerFactory.getLogger(KafkaClientTelemetry.class);
    private static final String DEFAULT_METRICS_TOPIC = "default-kafka-metrics";
    private static final String DEFAULT_BROKER_ADDRESS = "localhost:9092";
    private static final String METRICS_TOPIC_ENV = "KAFKA_METRICS_TOPIC";
    private static final String BROKER_ADDRESS_ENV = "KAFKA_BROKER_ADDRESS";

    private final Producer<String, String> producer;
    private final String topic;
    private final ObjectMapper objectMapper = new ObjectMapper();


    public KafkaClientTelemetry() {
        topic = System.getenv().getOrDefault(METRICS_TOPIC_ENV, DEFAULT_METRICS_TOPIC);
        producer = metricsProducer(System.getenv().getOrDefault(BROKER_ADDRESS_ENV, DEFAULT_BROKER_ADDRESS));
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
    }

    @Override
    public void metricChange(KafkaMetric kafkaMetric) {
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public ClientTelemetryReceiver clientReceiver() {
        return this;
    }

    @Override
    public void exportMetrics(AuthorizableRequestContext context, ClientTelemetryPayload payload) {
        try {
            String json = toJson(context, payload);
            producer.send(new ProducerRecord<>(
                    topic,
                    null,
                    "client_telemetry",
                    json,
                    List.of(new RecordHeader("via", "KIP-714".getBytes()))), (recordMetadata, e) -> {
                if (e != null) {
                    log.error("Error sending telemetry data to Kafka", e);
                } else {
                    log.info("Telemetry data sent to Kafka topic {} at offset {}", recordMetadata.topic(), recordMetadata.offset());
                }
            });
            log.info("Context: {},  Payload:{}", context, json);
        } catch (Exception e) {
            log.error("Could not process the metric", e);
        }
    }

    private String toJson(AuthorizableRequestContext context, ClientTelemetryPayload payload) throws InvalidProtocolBufferException, JsonProcessingException {
        Map<String, Object> eventData = new LinkedHashMap<>();
        eventData.put("clientInstanceId", payload.clientInstanceId().toString());
        eventData.put("isTerminating", payload.isTerminating());
        eventData.put("contentType", payload.contentType());

        // Add context information
        Map<String, Object> contextInfo = new LinkedHashMap<>();
        contextInfo.put("clientId", context.clientId());
        contextInfo.put("requestType", context.requestType());
        contextInfo.put("clientAddress", context.clientAddress().getHostAddress());
        contextInfo.put("listenerName", context.listenerName());
        eventData.put("context", contextInfo);

        // Parse and add metrics data
        MetricsData data = MetricsData.parseFrom(payload.data());
        Map<String, Object> metrics = new LinkedHashMap<>();

        for (ResourceMetrics resourceMetrics : data.getResourceMetricsList()) {
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                for (Metric metric : scopeMetrics.getMetricsList()) {
                    Map<String, Object> metricMap = new LinkedHashMap<>();
                    metricMap.put("name", metric.getName());
                    metricMap.put("description", metric.getDescription());
                    metricMap.put("unit", metric.getUnit());

                    switch (metric.getDataCase()) {
                        case SUM: {
                            Sum sum = metric.getSum();
                            metricMap.put("aggregationTemporality", sum.getAggregationTemporality().name());
                            List<Map<String, Object>> dataPoints = new ArrayList<>();
                            for (NumberDataPoint dp : sum.getDataPointsList()) {
                                Map<String, Object> dpMap = new LinkedHashMap<>();
                                if (dp.hasAsDouble()) dpMap.put("asDouble", dp.getAsDouble());
                                if (dp.hasAsInt()) dpMap.put("asInt", dp.getAsInt());
                                dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                List<Map<String, Object>> attributes = new ArrayList<>();
                                dp.getAttributesList().forEach(keyValue -> {
                                    Map<String, Object> attribute = new LinkedHashMap<>();
                                    attribute.put("key", keyValue.getKey());
                                    attribute.put("value", keyValue.getValue().getStringValue());
                                    attributes.add(attribute);
                                });
                                dpMap.put("attributes", attributes);
                                dataPoints.add(dpMap);
                            }
                            metricMap.put("dataPoints", dataPoints);
                            break;
                        }
                        case GAUGE: {
                            Gauge gauge = metric.getGauge();
                            List<Map<String, Object>> dataPoints = new ArrayList<>();
                            for (NumberDataPoint dp : gauge.getDataPointsList()) {
                                Map<String, Object> dpMap = new LinkedHashMap<>();
                                if (dp.hasAsDouble()) dpMap.put("asDouble", dp.getAsDouble());
                                if (dp.hasAsInt()) dpMap.put("asInt", dp.getAsInt());
                                dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                List<Map<String, Object>> attributes = new ArrayList<>();
                                dp.getAttributesList().forEach(keyValue -> {
                                    Map<String, Object> attribute = new LinkedHashMap<>();
                                    attribute.put("key", keyValue.getKey());
                                    attribute.put("value", keyValue.getValue().getStringValue());
                                    attributes.add(attribute);
                                });
                                dpMap.put("attributes", attributes);
                                dataPoints.add(dpMap);
                            }
                            metricMap.put("dataPoints", dataPoints);
                            break;
                        }
                        case HISTOGRAM: {
                            Histogram histogram = metric.getHistogram();
                            metricMap.put("aggregationTemporality", histogram.getAggregationTemporality().name());
                            List<Map<String, Object>> dataPoints = new ArrayList<>();
                            for (HistogramDataPoint dp : histogram.getDataPointsList()) {
                                Map<String, Object> dpMap = new LinkedHashMap<>();
                                dpMap.put("count", dp.getCount());
                                dpMap.put("sum", dp.getSum());
                                dpMap.put("bucketCounts", dp.getBucketCountsList());
                                dpMap.put("explicitBounds", dp.getExplicitBoundsList());
                                dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                List<Map<String, Object>> attributes = new ArrayList<>();
                                dp.getAttributesList().forEach(keyValue -> {
                                    Map<String, Object> attribute = new LinkedHashMap<>();
                                    attribute.put("key", keyValue.getKey());
                                    attribute.put("value", keyValue.getValue().getStringValue());
                                    attributes.add(attribute);
                                });
                                dpMap.put("attributes", attributes);
                                dataPoints.add(dpMap);
                            }
                            metricMap.put("dataPoints", dataPoints);
                            break;
                        }
                        case EXPONENTIAL_HISTOGRAM: {
                            ExponentialHistogram eh = metric.getExponentialHistogram();
                            metricMap.put("aggregationTemporality", eh.getAggregationTemporality().name());
                            List<Map<String, Object>> dataPoints = new ArrayList<>();
                            for (ExponentialHistogramDataPoint dp : eh.getDataPointsList()) {
                                Map<String, Object> dpMap = new LinkedHashMap<>();
                                dpMap.put("count", dp.getCount());
                                dpMap.put("sum", dp.getSum());
                                dpMap.put("scale", dp.getScale());
                                dpMap.put("zeroCount", dp.getZeroCount());
                                dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                List<Map<String, Object>> attributes = new ArrayList<>();
                                dp.getAttributesList().forEach(keyValue -> {
                                    Map<String, Object> attribute = new LinkedHashMap<>();
                                    attribute.put("key", keyValue.getKey());
                                    attribute.put("value", keyValue.getValue().getStringValue());
                                    attributes.add(attribute);
                                });
                                dpMap.put("attributes", attributes);
                                dataPoints.add(dpMap);
                            }
                            metricMap.put("dataPoints", dataPoints);
                            break;
                        }
                        case SUMMARY: {
                            Summary summary = metric.getSummary();
                            List<Map<String, Object>> dataPoints = new ArrayList<>();
                            for (SummaryDataPoint dp : summary.getDataPointsList()) {
                                Map<String, Object> dpMap = new LinkedHashMap<>();
                                dpMap.put("count", dp.getCount());
                                dpMap.put("sum", dp.getSum());
                                List<Map<String, Object>> quantiles = new ArrayList<>();
                                for (var q : dp.getQuantileValuesList()) {
                                    quantiles.add(Map.of(
                                            "quantile", q.getQuantile(),
                                            "value", q.getValue()
                                    ));
                                }
                                dpMap.put("quantileValues", quantiles);
                                dpMap.put("timeUnixNano", dp.getTimeUnixNano());
                                dpMap.put("startTimeUnixNano", dp.getStartTimeUnixNano());
                                List<Map<String, Object>> attributes = new ArrayList<>();
                                dp.getAttributesList().forEach(keyValue -> {
                                    Map<String, Object> attribute = new LinkedHashMap<>();
                                    attribute.put("key", keyValue.getKey());
                                    attribute.put("value", keyValue.getValue().getStringValue());
                                    attributes.add(attribute);
                                });
                                dpMap.put("attributes", attributes);
                                dataPoints.add(dpMap);
                            }
                            metricMap.put("dataPoints", dataPoints);
                            break;
                        }
                        case DATA_NOT_SET:
                            metricMap.put("data", "unknown");
                            break;
                    }
                    metrics.put(metric.getName(), metricMap);
                }
            }
        }
        eventData.put("metrics", metrics);
        return objectMapper.writeValueAsString(eventData);
    }

    private KafkaProducer<String, String> metricsProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384 * 10);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        return new KafkaProducer<>(props);
    }
}
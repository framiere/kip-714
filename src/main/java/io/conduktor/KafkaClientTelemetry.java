package io.conduktor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.opentelemetry.proto.metrics.v1.MetricsData.parseFrom;

public class KafkaClientTelemetry implements ClientTelemetry, MetricsReporter, ClientTelemetryReceiver {

    private static final Logger log = LoggerFactory.getLogger(KafkaClientTelemetry.class);
    private static final String DEFAULT_METRICS_TOPIC = "default-kafka-metrics";
    private static final String DEFAULT_BROKER_ADDRESS = "localhost:9092";
    private static final String METRICS_TOPIC_ENV = "KAFKA_METRICS_TOPIC";
    private static final String BROKER_ADDRESS_ENV = "KAFKA_BROKER_ADDRESS";

    private final Producer<String, String> producer;
    private final String topic;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();


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
        executor.shutdown();
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
        executor.submit(() -> {
            try {
                MetricsData data = parseFrom(payload.data());
                if (hasMetrics(data)) {
                    producer.send(new ProducerRecord<>(topic, null, context.clientId(), toJson(context, payload),
                            List.of(new RecordHeader("via", "KIP-714".getBytes()))), errorHandler());
                }
            } catch (Exception e) {
                log.error("Could not process the metric", e);
            }
        });
    }

    private String toJson(AuthorizableRequestContext context, ClientTelemetryPayload payload) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode metricsJson = mapper.createObjectNode();
        metricsJson.put("clientInstanceId", payload.clientInstanceId().toString());
        metricsJson.put("isTerminating", payload.isTerminating());
        metricsJson.put("contentType", payload.contentType());

        ObjectNode contextNode = mapper.createObjectNode();
        contextNode.put("clientId", context.clientId());
        contextNode.put("requestType", "" + ApiKeys.forId(context.requestType()));
        contextNode.put("requestVersion", context.requestVersion());
        contextNode.put("clientAddress", context.clientAddress().getHostAddress());
        contextNode.put("listenerName", context.listenerName());
        contextNode.put("principal", "" + context.principal());
        contextNode.put("securityProtocol", "" + context.securityProtocol());

        metricsJson.put("context", contextNode);
        metricsJson.objectNode().rawValueNode(new RawValue(JsonFormat.printer().print(parseFrom(payload.data()))));
        return mapper.writeValueAsString(metricsJson);
    }

    private KafkaProducer<String, String> metricsProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.ZSTD.name);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 1024 * 2);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        return new KafkaProducer<>(props);
    }

    private static boolean hasMetrics(MetricsData data) {
        for (ResourceMetrics resourceMetrics : data.getResourceMetricsList()) {
            for (ScopeMetrics scopeMetrics : resourceMetrics.getScopeMetricsList()) {
                if (!scopeMetrics.getMetricsList().isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Callback errorHandler() {
        return (recordMetadata, e) -> {
            if (e != null) {
                log.error("Error sending telemetry data to Kafka", e);
            }
        };
    }
}
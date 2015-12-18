/*
 * Copyright 2015 Ciena Networks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ciena.onos;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Modified;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.onlab.util.Tools;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.cluster.ClusterService;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceEvent.Type;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.link.LinkEvent;
import org.onosproject.net.link.LinkListener;
import org.onosproject.net.link.LinkService;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

/**
 * ONOS component that bridges device and link notifications to a Kafka message
 * bus.
 *
 * @author David K. Bainbridge (dbainbri@ciena.com)
 */
@Component(immediate = true)
public class KafkaNotificationBridge {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final static String DEVICE_TOPIC = "onos.device";
    private final static String LINK_TOPIC = "onos.link";

    /*
     * Property names used with ONOS to define properties that are used when
     * instantiating a RabbitMQ producer
     */
    private final static String PUBLISH_RABBIT_CONFIG = "publish.rabbit";
    private final static String RABBIT_HOST_CONFIG = "rabbit.host";
    private final static String RABBIT_PORT_CONFIG = "rabbit.port";

    /*
     * Property names used with ONOS to define properties that are used when
     * instantiating a kafka producer
     */
    private final static String PUBLISH_KAFKA_CONFIG = "publish.kafka";
    private final static String KAFKA_ACKS_CONFIG = "kafka.acks";
    private final static String KAFKA_BATCH_SIZE_CONFIG = "kafka.batch.size";
    private final static String KAFKA_BLOCK_ON_BUFFER_FULL_CONFIG = "kafka.block.on.buffer.full";
    private final static String KAFKA_BOOTSTRAP_SERVERS_CONFIG = "kafka.bootstrap.servers";
    private final static String KAFKA_BUFFER_MEMORY_CONFIG = "kafka.buffer.memory";
    private final static String KAFKA_CLIENT_ID_CONFIG = "kafka.client.id";
    private final static String KAFKA_COMPRESSION_TYPE_CONFIG = "kafka.compression.type";
    private final static String KAFKA_LINGER_MS_CONFIG = "kafka.linger.ms";
    private final static String KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_CONFIG = "kafka.max.in.flight.requests.per.connection";
    private final static String KAFKA_MAX_REQUEST_SIZE_CONFIG = "kafka.max.request.size";
    private final static String KAFKA_METADATA_FETCH_TIMEOUT_CONFIG = "kafka.metadata.fetch.timeout";
    private final static String KAFKA_METADATA_MAX_AGE_CONFIG = "kafka.metadata.max.age";
    private final static String KAFKA_METRICS_NUM_SAMPLES_CONFIG = "kafka.metrics.num.samples";
    private final static String KAFKA_METRICS_SAMPLE_WINDOW_MS_CONFIG = "kafka.metrics.sample.window.ms";
    private final static String KAFKA_RECEIVE_BUFFER_CONFIG = "kafka.receive.buffer";
    private final static String KAFKA_RECONNECT_BACKOFF_MS_CONFIG = "kafka.reconnect.backoff.ms";
    private final static String KAFKA_RETRIES_CONFIG = "kafka.retries";
    private final static String KAFKA_RETRY_BACKOFF_MS_CONFIG = "kafka.retry.backoff.ms";
    private final static String KAFKA_SEND_BUFFER_CONFIG = "kafka.send.buffer";
    private final static String KAFKA_TIMEOUT_CONFIG = "kafka.timeout";

    /*
     * Default values used for instantiating a kafka producer
     */
    private final static String DEFAULT_KAFKA_ACKS = "1";
    private final static String DEFAULT_KAFKA_BATCH_SIZE = "16384";
    private final static String DEFAULT_KAFKA_BLOCK_ON_BUFFER_FULL = "true";
    private final static String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String DEFAULT_KAFKA_BUFFER_MEMORY = "33554432";
    private final static String DEFAULT_KAFKA_CLIENT_ID = "";
    private final static String DEFAULT_KAFKA_COMPRESSION_TYPE = "none";
    private final static String DEFAULT_KAFKA_LINGER_MS = "1";
    private final static String DEFAULT_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "5";
    private final static String DEFAULT_KAFKA_MAX_REQUEST_SIZE = "1048576";
    private final static String DEFAULT_KAFKA_METADATA_FETCH_TIMEOUT = "60000";
    private final static String DEFAULT_KAFKA_METADATA_MAX_AGE = "300000";
    private final static String DEFAULT_KAFKA_METRICS_NUM_SAMPLES = "2";
    private final static String DEFAULT_KAFKA_METRICS_SAMPLE_WINDOW_MS = "30000";
    private final static String DEFAULT_KAFKA_RECEIVE_BUFFER = "32768";
    private final static String DEFAULT_KAFKA_RECONNECT_BACKOFF_MS = "10";
    private final static String DEFAULT_KAFKA_RETRIES = "0";
    private final static String DEFAULT_KAFKA_RETRY_BACKOFF_MS = "100";
    private final static String DEFAULT_KAFKA_SEND_BUFFER = "131072";
    private final static String DEFAULT_KAFKA_TIMEOUT = "30000";

    private final static String DEFAULT_RABBIT_HOST = "localhost";
    private final static String DEFAULT_RABBIT_PORT = "5672";

    private static final String DEFAULT_PUBLISH_KAFKA = "true";
    private static final String DEFAULT_PUBLISH_RABBIT = "false";

    private DeviceListener deviceListener = null;
    private LinkListener linkListener = null;
    private Callback closure = null;

    // For subscribing to device-related events
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected LinkService linkService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected MastershipService mastershipService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ClusterService clusterService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ComponentConfigService configService;

    /*
     * Rabbit MQ Configuration Properties
     */
    @Property(name = "publish.rabbit", value = DEFAULT_PUBLISH_RABBIT, label = "Determines is messages are published via RabbitMQ.")
    private String cfgPublishRabbit = DEFAULT_PUBLISH_RABBIT;
    private boolean cfgPublishRabbitVal = Boolean.parseBoolean(cfgPublishRabbit);

    @Property(name = "rabbit.host", value = DEFAULT_RABBIT_HOST, label = "Determines is messages are published via RabbitMQ.")
    private String cfgRabbitHost = DEFAULT_RABBIT_HOST;

    @Property(name = "rabbit.port", value = DEFAULT_RABBIT_PORT, label = "Determines is messages are published via RabbitMQ.")
    private String cfgRabbitPort = DEFAULT_RABBIT_PORT;

    /*
     * Kafka Configuration Properties
     */
    @Property(name = "publish.kafka", value = DEFAULT_PUBLISH_KAFKA, label = "Determines is messages are published via Kafka.")
    private String cfgPublishKafka = DEFAULT_PUBLISH_KAFKA;
    private boolean cfgPublishKafkaVal = Boolean.parseBoolean(cfgPublishKafka);

    @Property(name = "kafka.acks", value = DEFAULT_KAFKA_ACKS, label = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. This controls the durability of records that are sent")
    private String cfgKafkaAcks = DEFAULT_KAFKA_ACKS;

    @Property(name = "kafka.batch.size", value = DEFAULT_KAFKA_BATCH_SIZE, label = "The producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition. This helps performance on both the client and the server. This configuration controls the default batch size in bytes.")
    private String cfgKafkaBatchSize = DEFAULT_KAFKA_BATCH_SIZE;

    @Property(name = "kafka.block.on.buffer.full", value = DEFAULT_KAFKA_BLOCK_ON_BUFFER_FULL, label = "When our memory buffer is exhausted we must either stop accepting new records (block) or throw errors.")
    private String cfgKafkaBlockOnBufferFull = DEFAULT_KAFKA_BLOCK_ON_BUFFER_FULL;

    @Property(name = "kafka.bootstrap.servers", value = DEFAULT_KAFKA_BOOTSTRAP_SERVERS, label = "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping—this list only impacts the initial hosts used to discover the full set of servers")
    private String cfgKafkaBootstrapServers = DEFAULT_KAFKA_BOOTSTRAP_SERVERS;

    @Property(name = "kafka.buffer.memory", value = DEFAULT_KAFKA_BUFFER_MEMORY, label = "The total bytes of memory the producer can use to buffer records waiting to be sent to the server")
    private String cfgKafkaBufferMemory = DEFAULT_KAFKA_BUFFER_MEMORY;

    @Property(name = "kafka.client.id", value = DEFAULT_KAFKA_CLIENT_ID, label = "An id string to pass to the server when making requests.")
    private String cfgKafkaClientId = DEFAULT_KAFKA_CLIENT_ID;

    @Property(name = "kafka.compression.type", value = DEFAULT_KAFKA_COMPRESSION_TYPE, label = "Specify the final compression type for a given topic.")
    private String cfgKafkaCompressionType = DEFAULT_KAFKA_COMPRESSION_TYPE;

    @Property(name = "kafka.linger.ms", value = DEFAULT_KAFKA_LINGER_MS, label = "The producer groups together any records that arrive in between request transmissions into a single batched request.")
    private String cfgKafkaLingerMs = DEFAULT_KAFKA_LINGER_MS;

    @Property(name = "kafka.max.in.flight.requests.per.connection", value = DEFAULT_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, label = "The maximum number of unacknowledged requests the client will send on a single connection before blocking. ")
    private String cfgKafkaMaxInFlightRequestsPerConnection = DEFAULT_KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;

    @Property(name = "kafka.max.request.size", value = DEFAULT_KAFKA_MAX_REQUEST_SIZE, label = "The maximum size of a request. This is also effectively a cap on the maximum record size.")
    private String cfgKafkaMaxRequestSize = DEFAULT_KAFKA_MAX_REQUEST_SIZE;

    @Property(name = "kafka.metadata.fetch.timeout", value = DEFAULT_KAFKA_METADATA_FETCH_TIMEOUT, label = "The first time data is sent to a topic we must fetch metadata about that topic to know which servers host the topic's partitions. This fetch to succeed before throwing an exception back to the client.")
    private String cfgKafkaMetadataFetchTimeout = DEFAULT_KAFKA_METADATA_FETCH_TIMEOUT;

    @Property(name = "kafka.metadata.max.age", value = DEFAULT_KAFKA_METADATA_MAX_AGE, label = "he period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.")
    private String cfgKafkaMetadataMaxAge = DEFAULT_KAFKA_METADATA_MAX_AGE;

    @Property(name = "kafka.metrics.num.samples", value = DEFAULT_KAFKA_METRICS_NUM_SAMPLES, label = "The number of samples maintained to compute metrics.")
    private String cfgKafkaMetricsNumSamples = DEFAULT_KAFKA_METRICS_NUM_SAMPLES;

    @Property(name = "kafka.metrics.sample.window.ms", value = DEFAULT_KAFKA_METRICS_SAMPLE_WINDOW_MS, label = "The number of samples maintained to compute metrics.")
    private String cfgKafkaMetricsSampleWindowMs = DEFAULT_KAFKA_METRICS_SAMPLE_WINDOW_MS;

    @Property(name = "kafka.receive.buffer", value = DEFAULT_KAFKA_RECEIVE_BUFFER, label = "The size of the TCP receive buffer (SO_RCVBUF) to use when reading data.")
    private String cfgKafkaReceiveBuffer = DEFAULT_KAFKA_RECEIVE_BUFFER;

    @Property(name = "kafka.reconnect.backoff.ms", value = DEFAULT_KAFKA_RECONNECT_BACKOFF_MS, label = "The amount of time to wait before attempting to reconnect to a given host.")
    private String cfgKafkaReconnectBackoffMs = DEFAULT_KAFKA_RECONNECT_BACKOFF_MS;

    @Property(name = "kafka.retries", value = DEFAULT_KAFKA_RETRIES, label = "Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.")
    private String cfgKafkaRetries = DEFAULT_KAFKA_RETRIES;

    @Property(name = "kafka.retry.backoff.ms", value = DEFAULT_KAFKA_RETRY_BACKOFF_MS, label = "The amount of time to wait before attempting to retry a failed fetch request to a given topic partition.")
    private String cfgKafkaRetryBackoffMs = DEFAULT_KAFKA_RETRY_BACKOFF_MS;

    @Property(name = "kafka.send.buffer", value = DEFAULT_KAFKA_SEND_BUFFER, label = "The size of the TCP send buffer (SO_SNDBUF) to use when sending data.")
    private String cfgKafkaSendBuffer = DEFAULT_KAFKA_SEND_BUFFER;

    @Property(name = "kafka.timeout", value = DEFAULT_KAFKA_TIMEOUT, label = "The configuration controls the maximum amount of time the client will wait for the response of a request.")
    private String cfgKafkaTimeout = DEFAULT_KAFKA_TIMEOUT;

    private KafkaProducer<String, String> kafkaProducer = null;
    private Channel rabbitProducer = null;

    /**
     * Marshal a {@link org.onosproject.net.device.DeviceEvent} to a stringified
     * JSON object.
     *
     * @param event
     *            the device event to marshal
     * @return stringified JSON encoding of the device event
     */
    private String marshalEvent(DeviceEvent event) {
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        builder.append(String.format("\"type\":\"%s\",", event.type().toString()));
        builder.append(String.format("\"time\":%d,", event.time()));
        builder.append("\"subject\":{");
        builder.append(String.format("\"id\":\"%s\",", event.subject().id()));
        builder.append(String.format("\"chassis\":\"%s\",", event.subject().chassisId().toString()));
        builder.append(String.format("\"hw-version\":\"%s\",", event.subject().hwVersion()));
        builder.append(String.format("\"manufacturer\":\"%s\",", event.subject().manufacturer()));
        builder.append(String.format("\"serial-number\":\"%s\",", event.subject().serialNumber()));
        builder.append(String.format("\"sw-version\":\"%s\",", event.subject().swVersion()));
        builder.append(String.format("\"provider\":\"%s\",", event.subject().providerId().id()));
        builder.append(String.format("\"type\":\"%s\"", event.subject().type()));
        builder.append('}');
        if (event.port() != null) {
            builder.append(",\"port\":{");
            builder.append(String.format("\"type\":\"%s\",", event.port().type()));
            builder.append(String.format("\"number\":%d,", event.port().number().toLong()));
            builder.append(String.format("\"speed\":%d,", event.port().portSpeed()));
            builder.append(String.format("\"enabled\":%s", event.port().isEnabled()));
            builder.append('}');
        }
        builder.append('}');
        return builder.toString();
    }

    /**
     * Marshal a {@link org.onosproject.net.link.LinkEvent} to a stringified
     * JSON object.
     *
     * @param event
     *            the link event to marshal
     * @return stringified JSON encoding of the link event
     */
    private String marshalEvent(LinkEvent event) {
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        builder.append(String.format("\"type\":\"%s\",", event.type().toString()));
        builder.append(String.format("\"time\":%d,", event.time()));
        builder.append(String.format("\"src\":\"%s\",", event.subject().src().deviceId().toString()));
        builder.append(String.format("\"src-port\":%d,", event.subject().src().port().toLong()));
        builder.append(String.format("\"dst\":\"%s\",", event.subject().dst().deviceId().toString()));
        builder.append(String.format("\"dst-port\":%d,", event.subject().dst().port().toLong()));
        builder.append(String.format("\"state\":\"%s\",", event.subject().state()));
        builder.append(String.format("\"durable\":%s", event.subject().isDurable()));
        builder.append('}');
        return builder.toString();
    }

    /**
     * Called when component configuration options are modified and makes the
     * appropriate changes to the components implementation.
     *
     * @param context
     *            component context used to retrieve properties
     */
    @Modified
    public void modified(ComponentContext context) {
        Dictionary<?, ?> properties = context.getProperties();

        boolean isRabbitModified = false;
        String sVal = Strings.nullToEmpty(Tools.get(properties, PUBLISH_RABBIT_CONFIG)).trim();
        if (!sVal.isEmpty()) {
            boolean bVal = Boolean.parseBoolean(sVal);
            if (bVal != cfgPublishRabbitVal) {
                cfgPublishRabbit = sVal;
                cfgPublishRabbitVal = bVal;
                isRabbitModified = true;

                /*
                 * If rabbit is not being published and we have a rabbit
                 * connection, then close it
                 */
                if (!cfgPublishRabbitVal && rabbitProducer != null) {
                    try {
                        rabbitProducer.close();
                    } catch (IOException | TimeoutException e) {
                        log.error("Unable to close existing Rabbit channel", e);
                    } finally {
                        rabbitProducer = null;
                    }
                }
            }
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, RABBIT_HOST_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgRabbitHost)) {
            cfgRabbitHost = sVal;
            isRabbitModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, RABBIT_PORT_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgRabbitPort)) {
            cfgRabbitPort = sVal;
            isRabbitModified = true;
        }

        /*
         * Something has changed in the configuration, so grab the new Kafka
         * options from the properties and if any have changed then close and
         * re-create a new Kafka producer.
         */
        boolean isKafkaModified = false;
        sVal = Strings.nullToEmpty(Tools.get(properties, PUBLISH_KAFKA_CONFIG)).trim();
        if (!sVal.isEmpty()) {
            boolean bVal = Boolean.parseBoolean(sVal);
            if (bVal != cfgPublishKafkaVal) {
                cfgPublishKafka = sVal;
                cfgPublishKafkaVal = bVal;
                isKafkaModified = true;
                /*
                 * If rabbit is not being published and we have a rabbit
                 * connection, then close it
                 */
                if (!cfgPublishKafkaVal && kafkaProducer != null) {
                    try {
                        kafkaProducer.close();
                    } catch (Exception e) {
                        log.error("Unable to close existing Kafka channel", e);
                    } finally {
                        kafkaProducer = null;
                    }
                }
            }
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_ACKS_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaAcks)) {
            cfgKafkaAcks = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_BATCH_SIZE_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaBatchSize)) {
            cfgKafkaBatchSize = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_BLOCK_ON_BUFFER_FULL_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaBlockOnBufferFull)) {
            cfgKafkaBlockOnBufferFull = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_BOOTSTRAP_SERVERS_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaBootstrapServers)) {
            cfgKafkaBootstrapServers = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_BUFFER_MEMORY_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaBufferMemory)) {
            cfgKafkaBufferMemory = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_CLIENT_ID_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaClientId)) {
            cfgKafkaClientId = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_COMPRESSION_TYPE_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaCompressionType)) {
            cfgKafkaCompressionType = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_LINGER_MS_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaLingerMs)) {
            cfgKafkaLingerMs = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaMaxInFlightRequestsPerConnection)) {
            cfgKafkaMaxInFlightRequestsPerConnection = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_MAX_REQUEST_SIZE_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaMaxRequestSize)) {
            cfgKafkaMaxRequestSize = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_METADATA_FETCH_TIMEOUT_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaMetadataFetchTimeout)) {
            cfgKafkaMetadataFetchTimeout = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_METADATA_MAX_AGE_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaMetadataMaxAge)) {
            cfgKafkaMetadataMaxAge = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_METRICS_NUM_SAMPLES_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaMetricsNumSamples)) {
            cfgKafkaMetricsNumSamples = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_METRICS_SAMPLE_WINDOW_MS_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaMetricsSampleWindowMs)) {
            cfgKafkaMetricsSampleWindowMs = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_RECEIVE_BUFFER_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaReceiveBuffer)) {
            cfgKafkaReceiveBuffer = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_RECONNECT_BACKOFF_MS_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaReconnectBackoffMs)) {
            cfgKafkaReconnectBackoffMs = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_RETRIES_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaRetries)) {
            cfgKafkaRetries = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_RETRY_BACKOFF_MS_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaRetryBackoffMs)) {
            cfgKafkaRetryBackoffMs = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_SEND_BUFFER_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaSendBuffer)) {
            cfgKafkaSendBuffer = sVal;
            isKafkaModified = true;
        }

        sVal = Strings.nullToEmpty(Tools.get(properties, KAFKA_TIMEOUT_CONFIG)).trim();
        if (!sVal.isEmpty() && !sVal.equals(cfgKafkaTimeout)) {
            cfgKafkaTimeout = sVal;
            isKafkaModified = true;
        }

        if (isKafkaModified) {
            if (cfgPublishKafkaVal) {
                log.info("Kafka options have been modified, reconnecting with modified values");
                createKafkaProducer();
            } else {
                log.info(
                        "Kafka configuration options have been modified, but Kafka publishing is not active, so no action is being performed based on the modifications.");
            }
        } else {
            log.info(
                    "Kafka configuration options have been modified, but values have not been changed, so no action is being performed based on the modifications.");
        }

        if (isRabbitModified) {
            if (cfgPublishRabbitVal) {
                log.info("Rabbit options have been modified, reconnecting with modified values");
                createRabbitProducer();
            } else {
                log.info(
                        "Rabbit configuration options have been modified, but Rabbit publishing is not active, so no action is being performed based on the modifications.");
            }
        } else

        {
            log.info(
                    "Rabbit configuration options have been modified, but values have not been changed, so no action is being performed based on the modifications.");
        }

    }

    private synchronized void createRabbitProducer() {
        // Close and existing producer
        if (rabbitProducer != null) {
            try {
                rabbitProducer.close();
            } catch (IOException | TimeoutException e) {
                log.error("Unable to close existing Rabbit channel", e);
            } finally {
                rabbitProducer = null;
            }
        }

        log.error("Attempting to connect to RabbitMQ at {}:{} ", cfgRabbitHost, cfgRabbitPort);
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(cfgRabbitHost);
            factory.setPort(Integer.parseInt(cfgRabbitPort));
            rabbitProducer = factory.newConnection().createChannel();
        } catch (IOException | TimeoutException e) {
            log.error("Unable to create Rabbit producer to {}:{}, no events will be published on the Kafka bridge",
                    cfgRabbitHost, cfgRabbitPort, e);
        }
    }

    /**
     * Creates a KafkaProducer instances based on the configuration parameters
     * of the component. This producer is used internally for sending messages
     * over the Kafka bus.
     *
     * @param newKafkaServer
     *            the server and port to which the producer should be connected
     */
    private synchronized void createKafkaProducer() {

        // Close and existing producer
        if (kafkaProducer != null) {
            kafkaProducer.close();
            kafkaProducer = null;
        }

        log.debug("Attempting to connect to Kafka at {} as client {}", cfgKafkaBootstrapServers,
                clusterService.getLocalNode().id().toString());

        Properties props = new Properties();
        /*
         * Set the client ID to the local node ID if a value has not been set by
         * the configuration
         */
        props.put(ProducerConfig.CLIENT_ID_CONFIG, (Strings.isNullOrEmpty(cfgKafkaClientId)
                ? clusterService.getLocalNode().id().toString() : cfgKafkaClientId));

        /*
         * Set the serializer implementations, which is not something that can
         * be customized by the client.
         */
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /*
         * Set the properites that can be overriden by configuration
         */
        props.put(ProducerConfig.ACKS_CONFIG, cfgKafkaAcks);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, cfgKafkaBatchSize);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, cfgKafkaBlockOnBufferFull);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfgKafkaBootstrapServers);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, cfgKafkaBufferMemory);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, cfgKafkaCompressionType);
        props.put(ProducerConfig.LINGER_MS_CONFIG, cfgKafkaLingerMs);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, cfgKafkaMaxInFlightRequestsPerConnection);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, cfgKafkaMaxRequestSize);
        props.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, cfgKafkaMetadataFetchTimeout);
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, cfgKafkaMetadataMaxAge);
        props.put(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, cfgKafkaMetricsNumSamples);
        props.put(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, cfgKafkaMetricsSampleWindowMs);
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, cfgKafkaReceiveBuffer);
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, cfgKafkaReconnectBackoffMs);
        props.put(ProducerConfig.RETRIES_CONFIG, cfgKafkaRetries);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, cfgKafkaRetryBackoffMs);
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, cfgKafkaSendBuffer);
        props.put(ProducerConfig.TIMEOUT_CONFIG, cfgKafkaTimeout);

        try {
            kafkaProducer = new KafkaProducer<String, String>(props);
            log.info("Connected to Kafka at {} as client {}", cfgKafkaBootstrapServers,
                    clusterService.getLocalNode().id().toString());
        } catch (Exception e) {
            log.error("Unable to create Kafka producer to {}, no events will be published on the Kafka bridge",
                    cfgKafkaBootstrapServers, e);
        }
    }

    @Activate
    protected void activate(ComponentContext context) {
        log.info("Started");

        /*
         * Register component configuration properties so that they can be set
         * via the command line and will be properly synchronized in a clustered
         * environment.
         */
        if (configService != null) {
            configService.registerProperties(this.getClass());
        }

        /*
         * Update the configuration options, this will create message producers
         * as well.
         */
        if (context != null) {
            modified(context);
        }

        closure = new Callback() {
            @Override
            public void onCompletion(RecordMetadata meta, Exception e) {
                if (e != null) {
                    log.error("Failed to send message on topic {} : {}", meta.topic(), e);
                }
            }
        };

        deviceListener = new DeviceListener() {
            @Override
            public void event(DeviceEvent event) {

                /*
                 * Filtering out PPRT_STAT events (too many of them) and also
                 * only publishing if the subject of the event is mastered by
                 * the current instance so that we get some load balancing
                 * across instances in a clustered environment.
                 */
                if (event.type() != Type.PORT_STATS_UPDATED) {
                    if (mastershipService.isLocalMaster(event.subject().id())) {
                        String encoded = marshalEvent(event);
                        log.debug("SEND: (kafka = {}, rabbit = {}) {}", cfgPublishKafka, cfgPublishRabbit, encoded);
                        if (cfgPublishKafkaVal && kafkaProducer != null) {
                            kafkaProducer.send(new ProducerRecord<String, String>(DEVICE_TOPIC, encoded), closure);
                        }
                        if (cfgPublishRabbitVal && rabbitProducer != null) {
                            try {
                                rabbitProducer.basicPublish("", DEVICE_TOPIC, null, encoded.getBytes());
                            } catch (IOException e) {
                                log.error("Unexpected error while attempting to publish via Rabbit on topic {}",
                                        DEVICE_TOPIC, e);
                            }
                        }
                    } else {
                        log.debug("DROPPING DEVICE EVENT: not local master: {}",
                                mastershipService.getMasterFor(event.subject().id()));
                    }
                }
            }
        };

        linkListener = new LinkListener() {

            @Override
            public void event(LinkEvent event) {

                /*
                 * Only publish events if the destination of the link is
                 * mastered by the current instance so that we get some load
                 * balancing across instances in a clustered environment.
                 */
                if (mastershipService.isLocalMaster(event.subject().dst().deviceId())) {
                    String encoded = marshalEvent(event);
                    log.debug("SEND: (kafka = {}, rabbit = {}) {}", cfgPublishKafka, cfgPublishRabbit, encoded);
                    if (cfgPublishKafkaVal && kafkaProducer != null) {
                        kafkaProducer.send(new ProducerRecord<String, String>(LINK_TOPIC, marshalEvent(event)));
                    }
                    if (cfgPublishRabbitVal && rabbitProducer != null) {
                        try {
                            rabbitProducer.basicPublish("", LINK_TOPIC, null, encoded.getBytes());
                        } catch (IOException e) {
                            log.error("Unexpected error while attempting to publish via Rabbit on topic {}", LINK_TOPIC,
                                    e);

                        }
                    }
                } else {
                    log.debug("DROPPING LINK EVENT: not local master: {}",
                            mastershipService.getMasterFor(event.subject().src().deviceId()));
                }
            }
        };

        if (deviceService != null) {
            deviceService.addListener(deviceListener);
            log.info("Added device notification listener for Kafka bridge");
        } else {
            log.error(
                    "Unable to resolve device service and add device listener, no device events will be published on Kafka bridge");
        }

        if (linkService != null) {
            linkService.addListener(linkListener);
            log.info("Added link notification listener for Kafka bridge");
        } else {
            log.error(
                    "Unable to resolve link service and add link listener, no link events will be published on Kafka bridge");
        }
    }

    @Deactivate
    protected void deactivate() {
        log.info("Stopped");

        /*
         * Unregister kakfa bridge configuration information. The data will be
         * maintained so that it will be persisted over component installs and
         * activations.
         */
        if (configService != null) {
            configService.unregisterProperties(this.getClass(), false);
        }

        if (deviceService != null) {
            log.info("Removing device listener for Kafka bridge");
            deviceService.removeListener(deviceListener);
            deviceService = null;
        }

        if (linkService != null) {
            linkService.removeListener(linkListener);
            log.info("Removing link listener for Kafka bridge");
            linkService = null;
        }

        /*
         * Close the Kafka producer, no more events will be published.
         */
        if (kafkaProducer != null) {
            kafkaProducer.close();
            kafkaProducer = null;
        }

        if (rabbitProducer != null) {
            try {
                rabbitProducer.close();
            } catch (IOException | TimeoutException e) {
                log.error("Unexpected error while attempt to close connection to Rabbit", e);
            }
            rabbitProducer = null;
        }
    }
}

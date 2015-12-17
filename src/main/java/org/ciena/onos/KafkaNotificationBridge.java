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

import java.util.Dictionary;
import java.util.Properties;

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
	private final static String KAFKA_SERVER_CONFIG = "kafka-server";
	private final static String DEFAULT_KAFKA_SERVER = "localhost:9092";

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

	@Property(name = KAFKA_SERVER_CONFIG, value = DEFAULT_KAFKA_SERVER, label = "Kafka server for initial connection")
	private String kafkaServer = DEFAULT_KAFKA_SERVER;

	private KafkaProducer<String, String> producer = null;

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

		String value = Tools.get(properties, KAFKA_SERVER_CONFIG);
		String newKafkaServer = Strings.isNullOrEmpty(value) ? DEFAULT_KAFKA_SERVER : value.trim();
		if (!newKafkaServer.equals(kafkaServer)) {
			log.info("Kafka server updated from '{}' to '{}'", kafkaServer, newKafkaServer);
			createKafkaProducer(newKafkaServer);
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
	private synchronized void createKafkaProducer(String newKafkaServer) {

		// Close and existing producer
		if (producer != null) {
			producer.close();
			producer = null;
		}

		// If they didn't specify the new Kafka server to which to connect,
		// then this is either a re-connect or an initial connection
		if (newKafkaServer != null) {
			kafkaServer = newKafkaServer;
		}

		log.error("Attempting to connect to Kafka at {} as client {}", kafkaServer,
				clusterService.getLocalNode().id().toString());

		// TODO: These and others should be driven by configuration
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, clusterService.getLocalNode().id().toString());
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(ProducerConfig.RETRIES_CONFIG, 1);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		try {
			producer = new KafkaProducer<String, String>(props);
		} catch (Exception e) {
			log.error("Unable to create Kafka producer to {}, no events will be published on the Kafka bridge : {}",
					kafkaServer, e.getClass().getName(), e.getMessage(), e);
		}
	}

	@Activate
	protected void activate() {
		log.error("Started");

		/*
		 * Register component configuration properties so that they can be set
		 * via the command line and will be properly synchronized in a clustered
		 * environment.
		 */
		if (configService != null) {
			configService.registerProperties(this.getClass());
		}

		createKafkaProducer(null);

		closure = new Callback() {
			@Override
			public void onCompletion(RecordMetadata meta, Exception e) {
				if (e != null) {
					log.error("FAILED TO SEND MESSAGE on topic {} : {}", meta.topic(), e);
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
						if (producer != null) {
							String encoded = marshalEvent(event);
							log.error("SEND: {}", encoded);
							producer.send(new ProducerRecord<String, String>(DEVICE_TOPIC, encoded), closure);
						}
					} else {
						log.error("DROPPING DEVICE EVENT: not local master: {}",
								mastershipService.getMasterFor(event.subject().id()));
					}
				}
			}
		};

		linkListener = new LinkListener() {

			@Override
			public void event(LinkEvent event) {

				/*
				 * Only publish events if the destination of the link is mastered by
				 * the current instance so that we get some load balancing
				 * across instances in a clustered environment.
				 */
				if (mastershipService.isLocalMaster(event.subject().dst().deviceId())) {
					if (producer != null) {
						String encoded = marshalEvent(event);
						log.error("SEND: {}", encoded);
						producer.send(new ProducerRecord<String, String>(LINK_TOPIC, marshalEvent(event)));
					}
				} else {
					log.error("DROPPING DEVICE EVENT: not local master: {}",
							mastershipService.getMasterFor(event.subject().src().deviceId()));
				}
			}
		};

		if (deviceService != null) {
			deviceService.addListener(deviceListener);
			log.error("Added device notification listener for Kafka bridge");
		} else {
			log.error(
					"Unable to resolve device service and add device listener, no device events will be published on Kafka bridge");
		}

		if (linkService != null) {
			linkService.addListener(linkListener);
			log.error("Added link notification listener for Kafka bridge");
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
			log.error("Removing device listener for Kafka bridge");
			deviceService.removeListener(deviceListener);
			deviceService = null;
		}

		if (linkService != null) {
			linkService.removeListener(linkListener);
			log.error("Removing link listener for Kafka bridge");
			linkService = null;
		}

		/*
		 * Close the Kafka producer, no more events will be published.
		 */
		if (producer != null) {
			producer.close();
			producer = null;
		}
	}
}

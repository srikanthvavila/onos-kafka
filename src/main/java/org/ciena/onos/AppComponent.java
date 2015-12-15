/*
 * Copyright 2014 Open Networking Laboratory
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.onlab.util.Tools;
import org.onosproject.cfg.ComponentConfigService;
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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;

import com.google.common.base.Strings;

/**
 * Skeletal ONOS application component.
 */
@Component(immediate = true)
public class AppComponent {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final static String DEVICE_TOPIC = "onos.device";
	private final static String LINK_TOPIC = "onos.link";
	private final static String DEFAULT_KAFKA_SERVER = "localhost:9092";

	private DeviceListener deviceListener = null;
	private LinkListener linkListener = null;

	// For subscribing to device-related events
	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected DeviceService deviceService;

	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected LinkService linkService;

	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected MastershipService mastershipService;

	@Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
	protected ComponentConfigService configService;

	@Property(name = "kafkaServer", value = DEFAULT_KAFKA_SERVER, label = "Kafka server for initial connection")
	private String kafkaServer = DEFAULT_KAFKA_SERVER;

	private KafkaProducer<String, String> producer = null;

	private String marshalEvent(DeviceEvent event) {
		StringBuilder builder = new StringBuilder();
		builder.append('{');
		builder.append(String.format("\"%s\" : \"%s\",", "type", event.type().toString()));
		builder.append(String.format("\"%s\" : %d,", "time", event.time()));
		builder.append(String.format("\"%s\" : \"%s\",", "subject", event.subject().id()));
		builder.append(String.format("\"%s\" : %s", "port", event.port().number().toLong()));
		builder.append('}');
		return builder.toString();
	}

	@Modified
	public void modified(ComponentContext context) {
		Dictionary<?, ?> properties = context.getProperties();

		String value = Tools.get(properties, "kafkaServer");
		String newKafkaServer = Strings.isNullOrEmpty(value) ? DEFAULT_KAFKA_SERVER : value.trim();
		if (!newKafkaServer.equals(kafkaServer)) {
			log.info("Kafka server updated from '{}' to '{}'", kafkaServer, newKafkaServer);
			createKafkaProducer(newKafkaServer);
		}
	}

	private synchronized void createKafkaProducer(String newKafkaServer) {
		if (producer != null) {
			producer.close();
		}

		// If they didn't specify the new Kafka server to which to connect,
		// then this is either a re-connect or an initial connection
		if (newKafkaServer != null) {
			kafkaServer = newKafkaServer;
		}

		new DefaultPartitioner();
		log.error("DKB: Attempting to connect to KAFKA at {}", kafkaServer);
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaServer);
		// props.put("acks", "all");
		// props.put("retries", 0);
		// props.put("batch.size", 16384);
		// props.put("linger.ms", 1);
		// props.put("buffer.memory", 33554432);
		// props.put("serializer.class", "kafka.serializer.StringEncoder");

		// props.put("key.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		// props.put("value.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());

		try {
			producer = new KafkaProducer<String, String>(props);
		} catch (Throwable e) {
			log.error("Unable to connect to KAFKA at {} : {} : {}", kafkaServer, e.getClass().getName(),
					e.getMessage());
			e.printStackTrace();
			e.getCause().printStackTrace();
			if (e.getCause().getCause() != null) {
				e.getCause().getCause().printStackTrace();
			}
		}
	}

	@Activate
	protected void activate() {
		log.error("Started");

		if (configService != null) {
			configService.registerProperties(this.getClass());
		}

		createKafkaProducer(null);

		deviceListener = new DeviceListener() {

			@Override
			public void event(DeviceEvent event) {

				if (event.type() != Type.PORT_STATS_UPDATED && mastershipService.isLocalMaster(event.subject().id())) {
					log.error("**** DKB: DEVICE EVENT FOR: {}", event);
					if (producer != null) {
						producer.send(new ProducerRecord<String, String>(DEVICE_TOPIC, marshalEvent(event)));
					}
				}
			}
		};

		linkListener = new LinkListener() {

			@Override
			public void event(LinkEvent event) {
				if (mastershipService.isLocalMaster(event.subject().src().deviceId())) {
					log.error("**** DKB: LINK EVENT FOR: {}", event);
				}
			}
		};

		if (deviceService != null) {
			deviceService.addListener(deviceListener);
			log.error("ADDED DEVICE LISTENER");
		} else {
			log.error("Unable to resolve device service");
		}

		if (linkService != null) {
			linkService.addListener(linkListener);
			log.error("ADDED LINK LISTENER");
		} else {
			log.error("Unable to resolve link service");
		}
	}

	@Deactivate
	protected void deactivate() {

		if (configService != null) {
			configService.unregisterProperties(this.getClass(), false);
		}

		log.info("Stopped");
		if (deviceService != null) {
			log.error("REMOVED DEVICE LISTENER");
			deviceService.removeListener(deviceListener);
		} else {
			log.error("Unable to resolve device service");
		}
		deviceService = null;

		if (linkService != null) {
			linkService.removeListener(linkListener);
		} else {
			log.error("Unable to resolve link service");
		}
		linkService = null;

		if (producer != null) {
			producer.close();
			producer = null;
		}
	}
}

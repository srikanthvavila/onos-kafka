/*
 * Copyright 2016 Open Networking Lab
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
package org.ciena.onos.volt_event_publisher;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.onosproject.olt.AccessDeviceEvent;
import org.onosproject.olt.AccessDeviceListener;
import org.onosproject.olt.AccessDeviceService;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.ciena.onos.ext_notifier.api.PublisherRegistry;
import org.ciena.onos.ext_notifier.api.PublisherSource;
import org.ciena.onos.ext_notifier.api.Notifier;

/**
 * Publisher which publishes VOlt application events to external systems.
 */
@Component(immediate = true)
public class VOLTEventPublisher implements PublisherSource {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final static String publishes = "volt.events";

    // For subscribing to device-related events
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected AccessDeviceService oltService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PublisherRegistry<PublisherSource> publisherRegistry;

    protected AccessDeviceListener oltListener = null;

    @Activate
    protected void activate(ComponentContext context) {
        publisherRegistry.register(this);
        log.info("Activated");
    }

    @Deactivate
    protected void deactivate() {
        stop();
        log.info("Stopped");
    }

    @Override
    public String publishes() {
        return publishes;
    }

    /**
     * Marshal a {@link org.onosproject.net.device.DeviceEvent} to a stringified
     * JSON object.
     *
     * @param event
     *            the device event to marshal
     * @param userData
     *            User data in json format
     * @return stringified JSON encoding of the device event
     */
    private String encodeEvent(AccessDeviceEvent event, String userData) {
        switch (event.type()) {
        case DEVICE_CONNECTED:
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            builder.append(String.format("\"event_type\":\"volt.device\","));
            builder.append(String.format("\"priority\":\"INFO\","));
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            builder.append(String.format("\"timestamp\":\"%s\",", df.format(new Date())));
            builder.append("\"payload\":{");
            builder.append(String.format("\"id\":\"%s\"", event.subject().uri()));
            if (userData != null) {
                Map<String, String> userDataMap = new Gson().fromJson(userData,
                        new TypeToken<HashMap<String, String>>() {
                        }.getType());
                if (userDataMap != null) {
                    userDataMap.keySet().forEach((key) -> {
                        builder.append(String.format(",\"%s\":\"%s\"", key, userDataMap.get(key)));
                    });
                }
            }
            builder.append('}');
            builder.append('}');
            return builder.toString();
        case DEVICE_DISCONNECTED:
            break;
        case SUBSCRIBER_REGISTERED:
            builder = new StringBuilder();
            builder.append('{');
            builder.append(String.format("\"event_type\":\"volt.device.subscriber\","));
            builder.append(String.format("\"priority\":\"INFO\","));
            df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            builder.append(String.format("\"timestamp\":\"%s\",", df.format(new Date())));
            builder.append("\"payload\":{");
            builder.append(String.format("\"id\":\"%s\",", event.subject().uri()));
            builder.append(String.format("\"subscriber_id\":\"%s\"",
                    event.sVlanId().toString() + event.cVlanId().toString()));
            if (userData != null) {
                Map<String, String> userDataMap = new Gson().fromJson(userData,
                        new TypeToken<HashMap<String, String>>() {
                        }.getType());
                if (userDataMap != null) {
                    userDataMap.keySet().forEach((key) -> {
                        builder.append(String.format(",\"%s\":\"%s\"", key, userDataMap.get(key)));
                    });
                }
            }
            builder.append('}');
            builder.append('}');
            return builder.toString();
        case SUBSCRIBER_UNREGISTERED:
            break;
        }
        return null;
    }

    @Override
    public void start(Notifier notifier, String userData) {
        if (oltService != null) {
            oltListener = new AccessDeviceListener() {
                @Override
                public void event(AccessDeviceEvent event) {
                    String encoded = encodeEvent(event, userData);
                    if (encoded != null) {
                        notifier.publish(encoded);
                    }
                }
            };

            oltService.addListener(oltListener);
            log.info("Added OLT notification listener");
        } else {
            log.error(
                    "Unable to resolve OLT service and add OLT listener, no OLT events will be published on Kafka bridge");
        }
    }

    @Override
    public void stop() {
        if (oltListener != null) {
            oltService.removeListener(oltListener);
            log.info("Removing OLT listener for Kafka bridge");
            oltListener = null;
        }
    }
}
